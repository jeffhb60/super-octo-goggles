"""Serial NBA Stats client with immutable response snapshots and explicit cache reuse.

Design goals:
- Fetch one request at a time (serial), respecting a minimum interval between calls.
- Persist every fetched response as an immutable raw snapshot before validating it.
- Validate strictly; when `allow_partial=False`, any quarantined game aborts the call.
- When `allow_partial=True`, bad games are quarantined into `self.issues` and
  written to a `<snapshot>.quality.json` sidecar, and good rows are returned.
- Venue resolution can come from explicit `venue_overrides` or from the season
  schedule (fetched on demand when `resolve_schedule=True`).
"""
import json
import logging
import random
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

from .cache import atomic_json, candidates, save_snapshot, digest, read_snapshot
from .schedule import parse_schedule

log = logging.getLogger(__name__)


class DataError(ValueError):
    """Raised when an API response fails validation."""
    pass


# Columns we expect in every LeagueGameLog response.
# (Player mode adds PLAYER_ID.)
REQUIRED_COLUMNS = set(
    'SEASON_ID TEAM_ID GAME_ID GAME_DATE MATCHUP WL MIN '
    'FGM FGA FG_PCT FG3M FG3A FG3_PCT FTM FTA FT_PCT '
    'OREB DREB REB AST STL BLK TOV PF PTS PLUS_MINUS'.split()
)

# Regex for a valid 10-digit NBA game ID (e.g. "0022300001").
GAME_ID_PATTERN = re.compile(r'\d{10}')

# Regex for a valid season string like "2023-24".
SEASON_PATTERN = re.compile(r'\d{4}-\d{2}')


class NBAClient:
    def __init__(self, cache_dir='data/raw/api', timeout=60, attempts=3,
                 min_interval=2.0, fetcher=None, venue_overrides=None,
                 allow_partial=False, schedule_fetcher=None):
        # Validate configuration up front so bad settings fail fast.
        if attempts < 1 or min_interval < 0 or timeout <= 0:
            raise ValueError('Invalid client settings')

        self.cache = Path(cache_dir)
        self.timeout = timeout
        self.attempts = attempts
        self.interval = min_interval
        # Fetchers are injectable for testing; defaults hit the real nba_api.
        self.fetcher = fetcher or self._fetch
        self.schedule_fetcher = schedule_fetcher or self._fetch_schedule
        # Monotonic timestamp of the last request, used to throttle calls.
        self.last_request = None

        # Per-game venue overrides keyed by game ID; see _validate().
        self.venue_overrides = venue_overrides or {}
        # When False, any quarantined game raises instead of returning partial data.
        self.allow_partial = allow_partial

        # Populated by _process() / season_schedule(): quarantined issues,
        # the current snapshot path, and the parsed season schedule (if any).
        self.issues = []
        self.snapshot_path = None
        self.schedule_snapshot_path = None
        self.schedule = {}

    @staticmethod
    def _fetch(params, timeout):
        """Perform a LeagueGameLog request against the nba_api package."""
        from nba_api.stats.endpoints import LeagueGameLog
        return LeagueGameLog(timeout=timeout, **params).get_json()

    @staticmethod
    def _fetch_schedule(params, timeout):
        """Fetch a ScheduleLeagueV2 response as raw JSON.

        Uses the low-level HTTP helper directly rather than the endpoint
        wrapper, which avoids upstream dataframe-parser failures.
        """
        from nba_api.stats.library.http import NBAStatsHTTP
        return NBAStatsHTTP().send_api_request(
            endpoint='scheduleleaguev2',
            parameters=params,
            timeout=timeout,
        ).get_json()

    def season_schedule(self, season, reuse_cache=False):
        """Fetch (or reuse a cached) season schedule and parse it.

        Populates and returns `self.schedule` (a dict keyed by game ID).
        On success, `self.schedule_snapshot_path` points at the snapshot.
        """
        if (not SEASON_PATTERN.fullmatch(season)
                or int(season[-2:]) != (int(season[:4]) + 1) % 100):
            raise ValueError('Season must be YYYY-YY')

        self.schedule = {}
        self.schedule_snapshot_path = None

        params = {'Season': season, 'LeagueID': '00'}
        folder = self.cache / 'scheduleleaguev2' / season

        # Optional: reuse the newest valid cached schedule.
        if reuse_cache:
            for path in sorted(folder.glob('full_season_*.json'), reverse=True):
                try:
                    actual_params, _, raw = read_snapshot(path)
                    if actual_params == params:
                        self.schedule_snapshot_path = path
                        self.schedule = parse_schedule(raw, season)
                        return self.schedule
                except (ValueError, KeyError, TypeError, OSError):
                    # Skip unreadable or mismatched snapshots.
                    continue

        # Otherwise fetch with retries.
        for attempt in range(self.attempts):
            # Throttle: wait until `interval` seconds have elapsed since
            # the previous request.
            if self.last_request is not None:
                elapsed = time.monotonic() - self.last_request
                time.sleep(max(0, self.interval - elapsed))
            self.last_request = time.monotonic()

            try:
                raw = self.schedule_fetcher(params, self.timeout)
            except Exception:
                if attempt + 1 == self.attempts:
                    raise
                # Exponential backoff with jitter, capped at 60s.
                time.sleep(min(60, 2 ** (attempt + 1)) + random.random())
                continue
            break

        # Decode string bodies when possible, but keep malformed payloads.
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                pass

        # Persist the raw response as an immutable snapshot.
        stamp = datetime.now(timezone.utc)
        path = folder / f'full_season_{stamp:%Y%m%dT%H%M%S%fZ}.json'
        metadata = {
            'endpoint': 'scheduleleaguev2',
            'params': params,
            'fetched_at': stamp.isoformat(),
            'request_hash': digest(params),
            'response_hash': digest(raw),
            'schema_version': '1.0',
        }
        atomic_json(path, {'metadata': metadata, 'response': raw})

        self.schedule_snapshot_path = path
        self.schedule = parse_schedule(raw, season)
        return self.schedule

    def _validate(self, raw, mode, start, end, season):
        """Parse and validate a raw LeagueGameLog response.

        Returns the list of accepted row dicts. In team mode, invalid games
        are quarantined into `self.issues` rather than aborting (the caller
        decides whether partial results are acceptable). Structural problems
        with the response as a whole raise DataError.
        """
        # Reset per-call issue list.
        self.issues = []

        payload = json.loads(raw) if isinstance(raw, str) else raw

        # Normalize the "result sets" container to a list.
        result_sets = payload.get('resultSets', payload.get('resultSet', []))
        if isinstance(result_sets, dict):
            result_sets = [result_sets]

        # Exactly one LeagueGameLog set must be present.
        matches = [s for s in result_sets if s.get('name') == 'LeagueGameLog']
        if len(matches) != 1:
            raise DataError('Missing or ambiguous LeagueGameLog result set')

        headers = matches[0]['headers']
        rows = matches[0]['rowSet']

        # Column checks: no duplicates, and all required columns present.
        required = set(REQUIRED_COLUMNS)
        if mode == 'P':
            required.add('PLAYER_ID')
        if len(headers) != len(set(headers)) or not required <= set(headers):
            raise DataError(f'Unexpected columns; missing {required - set(headers)}')

        # An empty response can't establish completeness, so reject it.
        if not rows:
            raise DataError('Empty response; cannot establish completeness')

        records = []
        seen_keys = set()
        games = {}  # game_id -> list of rows for that game

        for row in rows:
            if len(row) != len(headers):
                raise DataError('Malformed row')

            r = dict(zip(headers, row))
            game_id = str(r['GAME_ID'])

            # Game ID must be a 10-digit number.
            if not GAME_ID_PATTERN.fullmatch(game_id):
                raise DataError(f'Invalid game ID: {game_id}')

            # Game date must fall within the requested window (if provided).
            game_date = date.fromisoformat(str(r['GAME_DATE'])[:10])
            if (start and game_date < start) or (end and game_date > end):
                raise DataError(f'Game outside requested date range: {game_id}')

            # Season must match the requested season.
            if str(r['SEASON_ID'])[-4:] != season[:4]:
                raise DataError(f'Unexpected season: {game_id}')

            # Team (and optionally player) identity must be positive integers.
            identity = [r['TEAM_ID']]
            if mode == 'P':
                identity.append(r['PLAYER_ID'])
            if any(not str(v).isdigit() or int(v) <= 0 for v in identity):
                raise DataError(f'Invalid team/player identity: {game_id}')

            # No duplicate (game, team[, player]) rows.
            key = (game_id, *map(str, identity))
            if key in seen_keys:
                raise DataError(f'Duplicate key: {key}')
            seen_keys.add(key)

            # Required outcome fields must be present.
            if r['WL'] not in ('W', 'L') or r['PTS'] is None:
                raise DataError(f'Incomplete result: {game_id}')

            games.setdefault(game_id, []).append(r)
            records.append(r)

        # In team mode, validate each home/away pair as a unit, resolve
        # venue info, and attach annotations. Games that fail are quarantined.
        if mode == 'T':
            accepted = []
            for game_id, pair in games.items():
                try:
                    self._validate_team_pair(game_id, pair)
                    accepted.extend(pair)
                except DataError as exc:
                    self.issues.append({
                        'game_id': game_id,
                        'reason': str(exc),
                        'rows': pair,
                    })
            if self.issues:
                log.warning('Quarantined %s games', len(self.issues))
            records = accepted

        return records

    def _validate_team_pair(self, game_id, pair):
        """Validate a single game's two team rows and annotate venue fields.

        Venue resolution order: explicit `venue_overrides` wins; otherwise
        the parsed season schedule (if available) supplies home/away and
        arena details. Mutates each row to add IS_HOME, NEUTRAL_SITE,
        VENUE_SOURCE, and (when the schedule is known) ARENA* fields.
        Raises DataError if the pair is inconsistent.
        """
        # A game must have exactly two rows with the same date.
        if len(pair) != 2 or len({r['GAME_DATE'] for r in pair}) != 1:
            raise DataError('Incomplete or inconsistent team pair')

        x, y = pair

        # Scores and win/loss flags must be mutually consistent.
        if (x['PTS'] == y['PTS']
                or x['WL'] == y['WL']
                or (x['PTS'] > y['PTS']) != (x['WL'] == 'W')):
            raise DataError('Inconsistent score')

        # If PLUS_MINUS is present, it must equal the point differential.
        for r, opponent in ((x, y), (y, x)):
            if (r['PLUS_MINUS'] is not None
                    and r['PLUS_MINUS'] != r['PTS'] - opponent['PTS']):
                raise DataError('Inconsistent point differential')

        # Default home/away designation comes from the MATCHUP string.
        home = [r for r in pair if ' vs. ' in str(r['MATCHUP'])]
        away = [r for r in pair if ' @ ' in str(r['MATCHUP'])]

        # Prefer an explicit override; otherwise fall back to the schedule.
        override = self.venue_overrides.get(game_id)
        scheduled = self.schedule.get(game_id)

        if scheduled and override is None:
            override = scheduled
            # The schedule must reference exactly the two teams in the log.
            schedule_teams = {
                str(scheduled['home_team_id']),
                str(scheduled['away_team_id']),
            }
            log_teams = {str(r['TEAM_ID']) for r in pair}
            if schedule_teams != log_teams:
                raise DataError('Schedule team IDs conflict with game log')

        if override is not None:
            # Explicit overrides must carry neutral_site and source; the
            # schedule records are trusted as-is.
            if (override is not scheduled
                    and not isinstance(override.get('neutral_site'), bool)):
                raise DataError('Venue override requires neutral_site and source')
            if not str(override.get('source', '')).strip():
                raise DataError('Venue override requires neutral_site and source')

            home = [r for r in pair
                    if str(r['TEAM_ID']) == str(override.get('home_team_id'))]
            away = [r for r in pair
                    if str(r['TEAM_ID']) == str(override.get('away_team_id'))]

        # Exactly one home and one away row are required, and they must differ.
        if len(home) != 1 or len(away) != 1 or home[0] is away[0]:
            raise DataError('Unknown home/away designation')

        # Annotate the rows so downstream code doesn't re-derive venue info.
        for r in (home[0], away[0]):
            r['IS_HOME'] = r is home[0]
            r['NEUTRAL_SITE'] = override['neutral_site'] if override else None
            r['VENUE_SOURCE'] = (
                override['source'] if override else 'LeagueGameLog.MATCHUP'
            )
            if scheduled:
                r['ARENA'] = scheduled['arena']
                r['ARENA_CITY'] = scheduled['city']
                r['ARENA_STATE'] = scheduled['state']
                r['ARENA_COUNTRY'] = scheduled['country']
                r['SCHEDULED_TIPOFF_UTC'] = scheduled['scheduled_tipoff_utc']

    def _process(self, raw, mode, start, end, season, path):
        """Validate a raw response, write a quality report, and enforce policy.

        Always writes `<path>.quality.json`. Raises DataError if validation
        fails structurally, if partial results are not allowed, or if no
        valid records remain.
        """
        self.snapshot_path = path
        self.issues = []
        try:
            records = self._validate(raw, mode, start, end, season)
        except (ValueError, KeyError, TypeError) as exc:
            # Structural failure: record it against the snapshot and re-raise.
            self.issues.append({'game_id': None, 'reason': str(exc)})
            self._report(path)
            raise

        self._report(path)

        # If games were quarantined and the caller asked for all-or-nothing,
        # fail now that the quality report has been written.
        if self.issues and not self.allow_partial:
            raise DataError(
                f'{len(self.issues)} games quarantined; see {path}.quality.json'
            )

        if not records:
            raise DataError('No valid records remain')
        return records

    def _report(self, path):
        """Write the current issues list to a `<path>.quality.json` sidecar."""
        report = Path(str(path) + '.quality.json')
        atomic_json(report, {'snapshot': str(path), 'issues': self.issues})

    def game_logs(self, season, season_type='Regular Season', mode='T',
                  start=None, end=None, reuse_cache=False,
                  resolve_schedule=False):
        """Fetch (or reuse a cached) LeagueGameLog response.

        Args:
            season: e.g. "2023-24".
            season_type: e.g. "Regular Season" or "Playoffs".
            mode: 'T' for team logs, 'P' for player logs.
            start/end: optional date bounds (inclusive).
            reuse_cache: if True, prefer the newest cached snapshot.
            resolve_schedule: if True, fetch the season schedule first and
                use it to resolve home/away and arena details.

        On success, `self.issues` and `self.snapshot_path` describe the
        response that produced the returned rows.
        """
        # Validate season format and that the two years are consecutive.
        if (not SEASON_PATTERN.fullmatch(season)
                or int(season[-2:]) != (int(season[:4]) + 1) % 100):
            raise ValueError('Season must be YYYY-YY')

        if mode not in ('T', 'P'):
            raise ValueError('Mode must be T or P')

        if start and end and start > end:
            raise ValueError('Start must precede end')

        # Reset any schedule state from a previous call.
        self.schedule = {}
        if resolve_schedule:
            self.season_schedule(season, reuse_cache=reuse_cache)

        params = dict(
            season=season,
            season_type_all_star=season_type,
            player_or_team_abbreviation=mode,
            league_id='00',
            date_from_nullable=start.strftime('%m/%d/%Y') if start else '',
            date_to_nullable=end.strftime('%m/%d/%Y') if end else '',
        )

        # Optional: try to reuse the newest valid cached raw snapshot.
        if reuse_cache:
            for _, path, raw in candidates(self.cache, params):
                log.info('Using cached response %s', path)
                return self._process(raw, mode, start, end, season, path)

        # Otherwise fetch with retries.
        for attempt in range(self.attempts):
            # Throttle: wait until `interval` seconds have elapsed since
            # the previous request.
            if self.last_request is not None:
                elapsed = time.monotonic() - self.last_request
                time.sleep(max(0, self.interval - elapsed))
            self.last_request = time.monotonic()

            try:
                raw = self.fetcher(params, self.timeout)
            except Exception as exc:
                log.warning('Request %s/%s failed: %s',
                            attempt + 1, self.attempts, exc)
                if attempt + 1 == self.attempts:
                    raise
                # Exponential backoff with jitter, capped at 60s.
                time.sleep(min(60, 2 ** (attempt + 1)) + random.random())
                continue

            # Persist the raw response BEFORE validating so the snapshot
            # is available for debugging even if validation fails.
            stamp = datetime.now(timezone.utc)
            path = save_snapshot(self.cache, params, stamp, raw)
            log.info('Saved raw response to %s', path)

            records = self._process(raw, mode, start, end, season, path)
            log.info('Validated %s rows', len(records))
            return records
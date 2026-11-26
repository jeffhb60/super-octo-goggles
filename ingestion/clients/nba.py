"""Serial NBA Stats client with immutable response snapshots and explicit cache reuse."""
import json
import logging
import random
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from .cache import atomic_json, candidates, save_snapshot

log = logging.getLogger(__name__)

class DataError(ValueError):
    pass

class NBAClient:
    def __init__(self, cache_dir='data/raw/api', timeout=60, attempts=3,
                 min_interval=2.0, fetcher=None, venue_overrides=None, allow_partial=False):
        if attempts < 1 or min_interval < 0 or timeout <= 0:
            raise ValueError('Invalid client settings')
        self.cache = Path(cache_dir)
        self.timeout, self.attempts, self.interval = timeout, attempts, min_interval
        self.fetcher = fetcher or self._fetch
        self.last_request = None
        self.venue_overrides = venue_overrides or {}
        self.allow_partial = allow_partial
        self.issues = []
        self.snapshot_path = None

    @staticmethod
    def _fetch(params, timeout):
        from nba_api.stats.endpoints import LeagueGameLog
        return LeagueGameLog(timeout=timeout, **params).get_json()

    def _validate(self, raw, mode, start, end, season):
        self.issues = []
        payload = json.loads(raw) if isinstance(raw, str) else raw
        sets = payload.get('resultSets', payload.get('resultSet', []))
        if isinstance(sets, dict):
            sets = [sets]
        matches = [s for s in sets if s.get('name') == 'LeagueGameLog']
        if len(matches) != 1:
            raise DataError('Missing or ambiguous LeagueGameLog result set')
        headers, rows = matches[0]['headers'], matches[0]['rowSet']
        required = set('SEASON_ID TEAM_ID GAME_ID GAME_DATE MATCHUP WL MIN FGM FGA FG_PCT FG3M FG3A FG3_PCT FTM FTA FT_PCT OREB DREB REB AST STL BLK TOV PF PTS PLUS_MINUS'.split())
        if mode == 'P':
            required.add('PLAYER_ID')
        if len(headers) != len(set(headers)) or not required <= set(headers):
            raise DataError(f'Unexpected columns; missing {required-set(headers)}')
        if not rows:
            raise DataError('Empty response; cannot establish completeness')
        records, keys, games = [], set(), {}
        for row in rows:
            if len(row) != len(headers):
                raise DataError('Malformed row')
            r = dict(zip(headers, row))
            g = str(r['GAME_ID'])
            if not re.fullmatch(r'\d{10}', g):
                raise DataError(f'Invalid game ID: {g}')
            d = date.fromisoformat(str(r['GAME_DATE'])[:10])
            if (start and d < start) or (end and d > end):
                raise DataError(f'Game outside requested date range: {g}')
            if str(r['SEASON_ID'])[-4:] != season[:4]:
                raise DataError(f'Unexpected season: {g}')
            identity = [r['TEAM_ID']] + ([r['PLAYER_ID']] if mode == 'P' else [])
            if any(not str(v).isdigit() or int(v) <= 0 for v in identity):
                raise DataError(f'Invalid team/player identity: {g}')
            key = (g, *map(str, identity))
            if key in keys:
                raise DataError(f'Duplicate key: {key}')
            keys.add(key)
            if r['WL'] not in ('W','L') or r['PTS'] is None:
                raise DataError(f'Incomplete result: {g}')
            games.setdefault(g, []).append(r)
            records.append(r)
        if mode == 'T':
            accepted = []
            for g, pair in games.items():
                try:
                    if len(pair) != 2 or len({r['GAME_DATE'] for r in pair}) != 1:
                        raise DataError('Incomplete or inconsistent team pair')
                    x, y = pair
                    if x['PTS'] == y['PTS'] or x['WL'] == y['WL'] or (x['PTS'] > y['PTS']) != (x['WL']=='W'):
                        raise DataError('Inconsistent score')
                    if any(r['PLUS_MINUS'] is not None and r['PLUS_MINUS'] != r['PTS']-o['PTS'] for r,o in ((x,y),(y,x))):
                        raise DataError('Inconsistent point differential')
                    home = [r for r in pair if ' vs. ' in str(r['MATCHUP'])]
                    away = [r for r in pair if ' @ ' in str(r['MATCHUP'])]
                    override = self.venue_overrides.get(g)
                    if override is not None:
                        if not isinstance(override.get('neutral_site'), bool) or not str(override.get('source','')).strip():
                            raise DataError('Venue override requires neutral_site and source')
                        home = [r for r in pair if str(r['TEAM_ID']) == str(override.get('home_team_id'))]
                        away = [r for r in pair if str(r['TEAM_ID']) == str(override.get('away_team_id'))]
                    if len(home) != 1 or len(away) != 1 or home[0] is away[0]:
                        raise DataError('Unknown home/away designation')
                    for r in (home[0], away[0]):
                        r['IS_HOME'] = r is home[0]
                        r['NEUTRAL_SITE'] = override['neutral_site'] if override else None
                        r['VENUE_SOURCE'] = override['source'] if override else 'LeagueGameLog.MATCHUP'
                    accepted.extend(pair)
                except DataError as exc:
                    self.issues.append({'game_id': g, 'reason': str(exc), 'rows': pair})
            if self.issues:
                log.warning('Quarantined %s games', len(self.issues))
            records = accepted
        return records

    def _process(self, raw, mode, start, end, season, path):
        self.snapshot_path = path
        self.issues = []
        try:
            records = self._validate(raw, mode, start, end, season)
        except (ValueError, KeyError, TypeError) as exc:
            self.issues.append({'game_id': None, 'reason': str(exc)})
            self._report(path)
            raise
        self._report(path)
        if self.issues and not self.allow_partial:
            raise DataError(f'{len(self.issues)} games quarantined; see {path}.quality.json')
        if not records:
            raise DataError('No valid records remain')
        return records

    def _report(self, path):
        report = Path(str(path) + '.quality.json')
        atomic_json(report, {'snapshot':str(path),'issues':self.issues})

    def game_logs(self, season, season_type='Regular Season', mode='T',
                  start=None, end=None, reuse_cache=False):
        if not re.fullmatch(r'\d{4}-\d{2}', season) or int(season[-2:]) != (int(season[:4])+1)%100:
            raise ValueError('Season must be YYYY-YY')
        if mode not in ('T','P'):
            raise ValueError('Mode must be T or P')
        if start and end and start > end:
            raise ValueError('Start must precede end')
        params = dict(season=season, season_type_all_star=season_type,
                      player_or_team_abbreviation=mode, league_id='00',
                      date_from_nullable=start.strftime('%m/%d/%Y') if start else '',
                      date_to_nullable=end.strftime('%m/%d/%Y') if end else '')
        if reuse_cache:
            for _, path, raw in candidates(self.cache, params):
                log.info('Using cached response %s', path)
                return self._process(raw, mode, start, end, season, path)
        for attempt in range(self.attempts):
            if self.last_request is not None:
                time.sleep(max(0, self.interval-(time.monotonic()-self.last_request)))
            self.last_request = time.monotonic()
            try:
                raw = self.fetcher(params, self.timeout)
            except Exception as exc:
                log.warning('Request %s/%s failed: %s', attempt+1, self.attempts, exc)
                if attempt+1 == self.attempts:
                    raise
                time.sleep(min(60, 2**(attempt+1)) + random.random())
                continue
            stamp = datetime.now(timezone.utc)
            path = save_snapshot(self.cache, params, stamp, raw)
            log.info('Saved raw response to %s', path)
            records = self._process(raw, mode, start, end, season, path)
            log.info('Validated %s rows', len(records))
            return records

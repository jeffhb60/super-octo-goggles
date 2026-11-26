"""Resumable, serial season collection with explicit coverage reporting.

For each requested season, this script:

1. Fetches (or reuses) the season schedule.
2. Fetches team and/or player game logs for each requested season type.
3. Cross-checks the fetched game logs against final games in the schedule
   to produce a coverage report.
4. Appends each task's result to a manifest JSON file, updating it after
   every task so an interrupted run can be resumed from the manifest.

Coverage is "verified" only when the schedule says the season is complete
and there are no missing, extra, wrong-team, or quarantined games;
otherwise it's "partial".
"""
import argparse
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from ingestion.clients.cache import atomic_json
from ingestion.clients.nba import NBAClient


# Game ID prefix -> season type. NBA game IDs encode their type:
# "002" = regular season, "004" = playoffs.
KINDS = {'Regular Season': '002', 'Playoffs': '004'}

# Schedule game status "3" means the game is final (played to completion).
FINAL_GAME_STATUS = '3'

# Regex for a valid season string like "2023-24".
SEASON_PATTERN = re.compile(r'\d{4}-\d{2}')

# Exit code used when any task failed or was only partially verified.
EXIT_NOT_CLEAN = 2


def _parse_season_start(value):
    """Validate a YYYY-YY season string and return its start year as int."""
    if (not SEASON_PATTERN.fullmatch(value)
            or int(value[-2:]) != (int(value[:4]) + 1) % 100):
        raise ValueError(f'Invalid season: {value}; use YYYY-YY')
    return int(value[:4])


def season_range(start, end):
    """Return all seasons from `start` to `end`, inclusive.

    Both endpoints are inclusive YYYY-YY strings, and start must not be
    after end.
    """
    start_year = _parse_season_start(start)
    end_year = _parse_season_start(end)
    if start_year > end_year:
        raise ValueError('Start season must precede end season')
    return [
        f'{year}-{(year + 1) % 100:02d}'
        for year in range(start_year, end_year + 1)
    ]


def current_season(today=None):
    """Return the current NBA season as YYYY-YY.

    The NBA season starts in October, so months before October belong to
    the season that began the previous calendar year.
    """
    today = today or date.today()
    start_year = today.year if today.month >= 10 else today.year - 1
    return f'{start_year}-{(start_year + 1) % 100:02d}'


def coverage(rows, schedule, kind, issues, team_rows=None):
    """Compute a coverage report for one (season, kind, mode) result.

    Args:
        rows: Fetched game-log rows (team or player).
        schedule: Parsed season schedule keyed by game ID.
        kind: 'Regular Season' or 'Playoffs' — selects the game ID prefix.
        issues: Quarantined issues reported by the client.
        team_rows: Optional team-mode rows to cross-check player rows against.

    Returns:
        A dict describing counts, missing/extra games, team mismatches,
        and an overall status of 'verified' or 'partial'.
    """
    prefix = KINDS[kind]

    # Final games of the requested kind, from the schedule.
    expected = {
        game_id: game
        for game_id, game in schedule.items()
        if (game_id.startswith(prefix)
            and str(game['game_status']) == FINAL_GAME_STATUS)
    }

    # Game ID -> set of team IDs actually observed in the rows.
    actual = defaultdict(set)
    for row in rows:
        actual[str(row['GAME_ID'])].add(str(row['TEAM_ID']))

    missing = sorted(expected.keys() - actual.keys())
    extra = sorted(actual.keys() - expected.keys())

    # Games where the observed teams don't match the scheduled home/away pair.
    wrong = sorted(
        game_id
        for game_id in actual.keys() & expected.keys()
        if actual[game_id] != {
            str(expected[game_id]['home_team_id']),
            str(expected[game_id]['away_team_id']),
        }
    )

    # If team rows are provided (player-mode runs), diff the (game, team)
    # pairs to spot player rows that don't line up with team coverage.
    team_mismatch = []
    if team_rows is not None:
        reference = {
            (str(r['GAME_ID']), str(r['TEAM_ID'])) for r in team_rows
        }
        observed = {
            (str(r['GAME_ID']), str(r['TEAM_ID'])) for r in rows
        }
        team_mismatch = sorted(reference ^ observed)

    result = dict(
        rows=len(rows),
        games=len(actual),
        expected_final_games=len(expected),
        missing_game_ids=missing,
        unexpected_game_ids=extra,
        team_mismatch_game_ids=wrong,
        team_log_pair_differences=team_mismatch,
        quarantined_game_ids=[issue.get('game_id') for issue in issues],
        coverage_basis='ScheduleLeagueV2 final games by game ID prefix',
        player_reference_available=team_rows is not None,
    )

    problems = missing or extra or wrong or team_mismatch or issues
    result['status'] = 'verified' if expected and not problems else 'partial'
    return result


def collect(client, seasons, manifest_path, *, kinds=tuple(KINDS),
            modes=('T', 'P'), refresh=False, active_season=None):
    """Collect schedules and game logs for `seasons`, writing a manifest.

    The manifest is rewritten after every task, so the run is resumable:
    rerunning with the same manifest path will simply append to it.

    Args:
        client: An NBAClient instance.
        seasons: Iterable of YYYY-YY season strings.
        manifest_path: Where to write the manifest JSON.
        kinds: Season types to collect.
        modes: Entity modes to collect ('T' and/or 'P').
        refresh: If True, always fetch; otherwise past seasons reuse cache.
        active_season: The season considered "current" (affects cache reuse
            and the "not started yet" playoff check).

    Returns:
        The final manifest dict.
    """
    active_season = active_season or current_season()

    report = {
        'version': '0.3.0',
        'started_at': datetime.now(timezone.utc).isoformat(),
        'seasons': list(seasons),
        'tasks': [],
        'finished_at': None,
    }

    def save(entry=None):
        """Append a task entry (if provided) and rewrite the manifest."""
        if entry is not None:
            report['tasks'].append(entry)
            print(
                f"{entry['season']} | {entry['dataset']} | {entry['status']}",
                flush=True,
            )
        atomic_json(Path(manifest_path), report)

    save()

    for season in report['seasons']:
        # Reuse cache only for past seasons, unless the caller asked to
        # refresh everything.
        reuse = not refresh and season < active_season

        schedule = {}
        client.schedule_snapshot_path = None

        # Fetch (or reuse) the season schedule first; every later task
        # depends on it.
        try:
            schedule = client.season_schedule(season, reuse_cache=reuse)
            save(dict(
                season=season,
                dataset='schedule',
                status='retrieved',
                games=len(schedule),
                snapshot=str(client.schedule_snapshot_path),
            ))
        except Exception as exc:
            save(dict(
                season=season,
                dataset='schedule',
                status='failed',
                error=str(exc),
            ))

        for kind in kinds:
            # Final games of this kind, used to short-circuit tasks for
            # seasons that haven't started yet.
            final_ids = [
                game
                for game in schedule.values()
                if (game['game_id'].startswith(KINDS[kind])
                    and str(game['game_status']) == FINAL_GAME_STATUS)
            ]

            team_rows = None  # filled in after the team-mode task

            for mode in modes:
                entry = dict(
                    season=season,
                    dataset=f'{kind}/{mode}',
                    mode=mode,
                    season_type=kind,
                    cache_reuse_allowed=reuse,
                    schedule_snapshot=(
                        str(client.schedule_snapshot_path)
                        if client.schedule_snapshot_path else None
                    ),
                )

                # Skip playoff collection for the current season if no
                # final playoff games are scheduled yet.
                if (season == active_season
                        and schedule
                        and not final_ids
                        and kind == 'Playoffs'):
                    save(dict(
                        entry,
                        status='not_started',
                        reason='No final playoff games in current schedule',
                    ))
                    continue

                client.snapshot_path = None
                client.issues = []

                try:
                    rows = client.game_logs(
                        season, kind, mode,
                        reuse_cache=reuse,
                        schedule_data=schedule,
                    )
                    if mode == 'T':
                        team_rows = rows

                    entry.update(coverage(
                        rows, schedule, kind, client.issues,
                        team_rows if mode == 'P' else None,
                    ))

                    # Player coverage needs team rows as a reference; if we
                    # asked for players but never collected teams, mark it
                    # partial rather than pretending it's verified.
                    if mode == 'P' and 'T' in modes and team_rows is None:
                        entry['status'] = 'partial'
                        entry['reason'] = 'Team reference unavailable'
                except Exception as exc:
                    entry.update(status='failed', error=str(exc))

                entry['snapshot'] = (
                    str(client.snapshot_path) if client.snapshot_path else None
                )
                quality_path = (
                    Path(str(client.snapshot_path) + '.quality.json')
                    if client.snapshot_path else None
                )
                entry['quality_report'] = (
                    str(quality_path) if quality_path and quality_path.exists()
                    else None
                )
                save(entry)

    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    report['counts'] = dict(Counter(t['status'] for t in report['tasks']))
    save()
    return report


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--start-season', required=True,
        help='First season to collect (YYYY-YY).',
    )
    parser.add_argument(
        '--end-season', default=current_season(),
        help='Last season to collect, inclusive (default: current season).',
    )
    parser.add_argument(
        '--cache-dir', type=Path, default=Path('data/raw/api'),
        help='Cache root directory (default: %(default)s).',
    )
    parser.add_argument(
        '--manifest-dir', type=Path, default=Path('data/manifests'),
        help='Where to write manifest files (default: %(default)s).',
    )
    parser.add_argument(
        '--season-type', choices=['all', 'regular', 'playoffs'], default='all',
        help='Which season types to collect (default: %(default)s).',
    )
    parser.add_argument(
        '--entity', choices=['all', 'teams', 'players'], default='all',
        help='Which entity modes to collect (default: %(default)s).',
    )
    parser.add_argument(
        '--refresh', action='store_true',
        help='Fetch all requested seasons again instead of reusing cache.',
    )
    parser.add_argument(
        '--min-interval', type=float, default=2.0,
        help='Minimum seconds between API requests (default: %(default)s).',
    )
    parser.add_argument(
        '--timeout', type=float, default=60,
        help='Request timeout in seconds (default: %(default)s).',
    )
    parser.add_argument(
        '--attempts', type=int, default=3,
        help='Request retry attempts (default: %(default)s).',
    )
    parser.add_argument(
        '--venue-overrides', type=Path,
        help='Path to a JSON file of venue overrides.',
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Validate inputs and build the client up front so config errors are
    # reported before any network activity.
    try:
        seasons = season_range(args.start_season, args.end_season)
        if seasons[-1] > current_season():
            raise ValueError('End season is in the future')

        overrides = (
            json.loads(args.venue_overrides.read_text(encoding='utf-8'))
            if args.venue_overrides else None
        )

        client = NBAClient(
            args.cache_dir,
            timeout=args.timeout,
            attempts=args.attempts,
            min_interval=args.min_interval,
            allow_partial=True,
            venue_overrides=overrides,
        )
    except (ValueError, OSError) as exc:
        parser.error(str(exc))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    # Translate CLI choices into the kinds/modes the collector expects.
    kinds = tuple(KINDS) if args.season_type == 'all' else (
        'Regular Season' if args.season_type == 'regular' else 'Playoffs',
    )
    modes = ('T', 'P') if args.entity == 'all' else (
        'T' if args.entity == 'teams' else 'P',
    )

    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    path = args.manifest_dir / (
        f'historical_{seasons[0]}_to_{seasons[-1]}_{stamp}_{uuid4().hex[:8]}.json'
    )
    print(f'Manifest: {path}', flush=True)

    report = collect(
        client, seasons, path,
        kinds=kinds, modes=modes, refresh=args.refresh,
    )
    print(json.dumps(report['counts'], indent=2))

    # Exit non-zero if any task failed or was only partially verified.
    return (
        EXIT_NOT_CLEAN
        if any(t['status'] in ('failed', 'partial') for t in report['tasks'])
        else 0
    )


if __name__ == '__main__':
    raise SystemExit(main())
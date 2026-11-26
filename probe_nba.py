"""Command-line probe for the NBA Stats client.

Fetches a LeagueGameLog response, prints a short summary, and reports any
data-quality issues the client quarantined. Exits with status 2 if issues
were found, and performs a known-count coverage check for the 2022-23
regular season.
"""
import argparse
import json
import logging
from datetime import date
from pathlib import Path

from ingestion.clients.nba import NBAClient

# Expected number of games in the 2022-23 regular season. Used only as a
# probe; other queries aren't checked against a known total.
EXPECTED_2022_23_REGULAR_SEASON_GAMES = 1230

# Exit code used when the client quarantined one or more games.
EXIT_QUARANTINE = 2

def current_season(today=None):
    """Return the current NBA season as YYYY-YY.

    The NBA season starts in October, so months before October belong to
    the season that began the previous calendar year.
    """
    today = today or date.today()
    start_year = today.year if today.month >= 10 else today.year - 1
    return f'{start_year}-{(start_year + 1) % 100:02d}'


def load_venue_overrides(path):
    """Load a venue-overrides JSON file, or return None if not provided."""
    if path is None:
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch and summarize an NBA LeagueGameLog response.'
    )
    parser.add_argument(
        '--season',
        default=current_season(),
        help='Season in YYYY-YY form (default: current season).',
    )
    parser.add_argument(
        '--season-type',
        default='Regular Season',
        help='Season type, e.g. "Regular Season" or "Playoffs" '
             '(default: %(default)s).',
    )
    parser.add_argument(
        '--players',
        action='store_true',
        help='Fetch player-level logs (mode P) instead of team-level (mode T).',
    )
    parser.add_argument(
        '--start',
        type=date.fromisoformat,
        help='Start date (inclusive), ISO format YYYY-MM-DD.',
    )
    parser.add_argument(
        '--end',
        type=date.fromisoformat,
        help='End date (inclusive), ISO format YYYY-MM-DD.',
    )
    parser.add_argument(
        '--reuse-cache',
        action='store_true',
        help='Reuse a valid cached snapshot instead of making a new request.',
    )
    parser.add_argument(
        '--venue-overrides',
        type=Path,
        help='Path to a JSON file of venue overrides.',
    )
    return parser.parse_args(argv)


def summarize(rows):
    """Print basic statistics about the fetched rows and return unique game IDs."""
    games = {r['GAME_ID'] for r in rows}

    print('Rows:', len(rows), 'Unique games:', len(games))
    print(
        'Date range:',
        min(r['GAME_DATE'] for r in rows),
        max(r['GAME_DATE'] for r in rows),
    )
    print('Columns:', ', '.join(rows[0]))

    return games


def check_2022_23_coverage(games):
    """Sanity-check that the 2022-23 regular season has the expected game count."""
    if len(games) != EXPECTED_2022_23_REGULAR_SEASON_GAMES:
        raise SystemExit(
            'Coverage check failed: expected '
            f'{EXPECTED_2022_23_REGULAR_SEASON_GAMES} games, got {len(games)}'
        )


def report_quarantined_issues(client):
    """Print quarantined games and exit with status 2 if any were found."""
    if not client.issues:
        return

    print('Quarantined games:', len(client.issues))
    for issue in client.issues:
        print(issue['game_id'], issue['reason'])
    print('Quality report:', str(client.snapshot_path) + '.quality.json')
    raise SystemExit(EXIT_QUARANTINE)


if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    client = NBAClient(
        # Partial results are allowed; bad games are quarantined in
        # `client.issues` rather than aborting the whole fetch.
        allow_partial=True,
        venue_overrides=load_venue_overrides(args.venue_overrides),
    )

    rows = client.game_logs(
        season=args.season,
        season_type=args.season_type,
        mode='P' if args.players else 'T',
        start=args.start,
        end=args.end,
        reuse_cache=args.reuse_cache,
    )

    games = summarize(rows)

    # Only check coverage for the specific query we know the answer to:
    # the full 2022-23 regular season with no date filtering.
    is_default_full_season = (
        args.season == '2022-23'
        and args.season_type == 'Regular Season'
        and not args.start
        and not args.end
    )
    if is_default_full_season:
        check_2022_23_coverage(games)

    # Any quarantined games indicate data-quality problems; report them
    # and exit non-zero before declaring the probe passed.
    report_quarantined_issues(client)

    print('Probe passed; full coverage is not established for other queries.')
"""Offline, manifest-driven rebuild of the analytical NBA database.

Reads a completed collection manifest, re-validates every referenced
snapshot against the season schedule, and loads the results into a
DuckDB database. Never touches the network: any accidental fetch attempt
raises immediately.

The manifest must reference only 'verified', 'retrieved', or
'not_started' tasks — partial or failed tasks must be recollected first.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from ingestion.clients.cache import read_snapshot, digest
from ingestion.clients.nba import NBAClient
from ingestion.clients.schedule import parse_schedule
from ingestion.collectors.historical import coverage, KINDS


# Stat columns copied from each game log into the stats tables.
STATS = (
    'MIN FGM FGA FG_PCT FG3M FG3A FG3_PCT FTM FTA FT_PCT '
    'OREB DREB REB AST STL BLK TOV PF PTS PLUS_MINUS'
).split()

# The same stat columns rendered as DuckDB DDL fragments.
STAT_SQL = ', '.join(f'{stat.lower()} DOUBLE' for stat in STATS)

# Full database schema. Loaded into a brand-new database file each build.
SCHEMA = f'''
CREATE TABLE snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    path VARCHAR NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    response_hash VARCHAR NOT NULL,
    request_hash VARCHAR NOT NULL
);
CREATE TABLE teams (
    team_id BIGINT PRIMARY KEY,
    abbreviation VARCHAR,
    name VARCHAR
);
CREATE TABLE players (
    player_id BIGINT PRIMARY KEY,
    name VARCHAR
);
CREATE TABLE games (
    game_id VARCHAR PRIMARY KEY,
    season VARCHAR NOT NULL,
    season_type VARCHAR NOT NULL,
    game_date DATE,
    scheduled_tipoff_utc TIMESTAMPTZ,
    game_status INTEGER,
    home_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    away_team_id BIGINT NOT NULL REFERENCES teams(team_id),
    neutral_site BOOLEAN,
    arena VARCHAR,
    arena_city VARCHAR,
    arena_state VARCHAR,
    arena_country VARCHAR,
    venue_source VARCHAR,
    home_points INTEGER,
    away_points INTEGER,
    schedule_snapshot_id VARCHAR REFERENCES snapshots(snapshot_id),
    CHECK (home_team_id <> away_team_id)
);
CREATE TABLE team_game_stats (
    game_id VARCHAR REFERENCES games(game_id),
    team_id BIGINT REFERENCES teams(team_id),
    matchup VARCHAR,
    wl VARCHAR,
    is_home BOOLEAN NOT NULL,
    {STAT_SQL},
    snapshot_id VARCHAR NOT NULL REFERENCES snapshots(snapshot_id),
    PRIMARY KEY (game_id, team_id)
);
CREATE TABLE player_game_stats (
    game_id VARCHAR,
    team_id BIGINT,
    player_id BIGINT REFERENCES players(player_id),
    matchup VARCHAR,
    wl VARCHAR,
    {STAT_SQL},
    snapshot_id VARCHAR NOT NULL REFERENCES snapshots(snapshot_id),
    PRIMARY KEY (game_id, team_id, player_id),
    FOREIGN KEY (game_id, team_id) REFERENCES team_game_stats(game_id, team_id)
);
CREATE TABLE build_metadata (
    schema_version VARCHAR,
    built_at TIMESTAMPTZ,
    manifest VARCHAR
);
CREATE VIEW game_results AS
    SELECT *,
        CASE WHEN home_points IS NOT NULL AND away_points IS NOT NULL
             THEN CAST(home_points > away_points AS INTEGER)
        END AS home_win,
        home_points - away_points AS mov
    FROM games;
'''

# Exit code used when the build fails.
EXIT_BUILD_FAILED = 2

# Tables in insertion order (so foreign keys resolve).
TABLE_ORDER = (
    'snapshots',
    'teams',
    'players',
    'games',
    'team_game_stats',
    'player_game_stats',
)


def _read_offline_fetcher(*args, **kwargs):
    """Fetcher stub used to guarantee the build never touches the network."""
    raise RuntimeError('Offline only')


def prepare(manifest_path, overrides=None):
    """Validate exact manifest snapshots, never fetch or rewrite raw data.

    Returns a dict of table name -> list of rows, ready to load.
    Raises ValueError if the manifest is unfinished, contains failed or
    partial tasks, references inconsistent snapshots, or fails
    revalidation against the season schedule.
    """
    manifest_path = Path(manifest_path)
    report = json.loads(manifest_path.read_text(encoding='utf-8'))

    # The manifest must be complete and contain at least one task.
    if not report.get('finished_at') or not report.get('tasks'):
        raise ValueError('Manifest is unfinished or empty')

    tasks = report['tasks']

    # Reject any task that isn't clean; the operator must recollect first.
    if any(t['status'] not in ('verified', 'retrieved', 'not_started')
           for t in tasks):
        raise ValueError(
            'Manifest contains partial or failed tasks; resolve and recollect first'
        )

    # Reject duplicate (season, dataset) entries.
    seen = set()
    for t in tasks:
        key = (t['season'], t['dataset'])
        if key in seen:
            raise ValueError(f'Duplicate manifest task: {key}')
        seen.add(key)

    # Output buckets, keyed by table name.
    out = {
        k: [] for k in
        ('snapshots', 'teams', 'players', 'games',
         'team_game_stats', 'player_game_stats')
    }

    # Accumulators for identity tables, keyed by ID for dedup.
    teams = {}
    players = {}
    games = {}
    sources = {}

    def read(path):
        """Read a snapshot, compute its stable ID, and record its source row."""
        p = Path(path)
        params, stamp, raw = read_snapshot(p)
        sid = digest([params, stamp.isoformat(), digest(raw)])
        sources[sid] = (sid, str(p), stamp, digest(raw), digest(params))
        return params, raw, sid

    # Client is used only for its validators; the offline fetcher makes
    # any accidental network call fail loudly.
    client = NBAClient(
        allow_partial=False,
        venue_overrides=overrides or {},
        fetcher=_read_offline_fetcher,
    )

    for season in sorted({t['season'] for t in tasks}):
        season_tasks = [t for t in tasks if t['season'] == season]

        # Each season must reference exactly one retrieved schedule.
        schedules = [
            t for t in season_tasks
            if t['dataset'] == 'schedule' and t['status'] == 'retrieved'
        ]
        if len(schedules) != 1:
            raise ValueError(f'{season}: exactly one schedule required')

        schedule_path = schedules[0]['snapshot']
        params, raw, schedule_snapshot_id = read(schedule_path)
        if params != {'Season': season, 'LeagueID': '00'}:
            raise ValueError('Schedule request mismatch')

        schedule = parse_schedule(raw, season)
        # Install the schedule on the client so _validate can annotate
        # home/away and arena fields.
        client.schedule = schedule

        for kind, prefix in KINDS.items():
            # Verified tasks for this season/kind, one per mode.
            group = [
                t for t in season_tasks
                if t.get('season_type') == kind and t['status'] == 'verified'
            ]
            by_mode = {t['mode']: t for t in group}
            if len(by_mode) != len(group) or set(by_mode) - {'T', 'P'}:
                raise ValueError('Unexpected dataset modes')

            # Player rows have a FK to team rows in this schema, so we
            # can't load players without their matching team logs.
            if 'P' in by_mode and 'T' not in by_mode:
                raise ValueError(
                    'Player import requires verified team logs in the same manifest'
                )

            team_rows = None

            for mode in ('T', 'P'):
                if mode not in by_mode:
                    continue

                task = by_mode[mode]

                # The task must have used the same schedule snapshot we're
                # about to load against.
                if (Path(task['schedule_snapshot']).resolve()
                        != Path(schedule_path).resolve()):
                    raise ValueError('Manifest schedule reference mismatch')

                params, raw, log_snapshot_id = read(task['snapshot'])

                # Full-season snapshots only: this build has no notion of
                # incremental loads.
                expected_params = dict(
                    season=season,
                    season_type_all_star=kind,
                    player_or_team_abbreviation=mode,
                    league_id='00',
                    date_from_nullable='',
                    date_to_nullable='',
                )
                if params != expected_params:
                    raise ValueError(
                        'Game log request mismatch; full-season snapshots required'
                    )

                # Re-run validation against the snapshot.
                rows = client._validate(raw, mode, None, None, season)

                # Re-check coverage against the schedule; the manifest's own
                # "verified" status is not trusted on its own.
                check = coverage(
                    rows, schedule, kind, client.issues,
                    team_rows if mode == 'P' else None,
                )
                if check['status'] != 'verified':
                    raise ValueError(
                        f'{season} {kind} {mode}: coverage failed on revalidation'
                    )

                if mode == 'T':
                    team_rows = rows

                for row in rows:
                    game_id = str(row['GAME_ID'])
                    team_id = int(row['TEAM_ID'])

                    # Remember team identity; keep any prior abbreviation
                    # or name if this row lacks them.
                    prior = teams.get(team_id, (None, None, None))
                    teams[team_id] = (
                        team_id,
                        row.get('TEAM_ABBREVIATION') or prior[1],
                        row.get('TEAM_NAME') or prior[2],
                    )

                    stats = [row[k] for k in STATS]

                    if mode == 'T':
                        scheduled = schedule[game_id]

                        # First team row for a game creates the games row.
                        if game_id not in games:
                            games[game_id] = [
                                game_id, season, kind,
                                row['GAME_DATE'],
                                scheduled['scheduled_tipoff_utc'],
                                scheduled['game_status'],
                                None, None,          # home/away team_id
                                row['NEUTRAL_SITE'],
                                scheduled['arena'],
                                scheduled['city'],
                                scheduled['state'],
                                scheduled['country'],
                                row['VENUE_SOURCE'],
                                None, None,          # home/away points
                                schedule_snapshot_id,
                            ]
                        game = games[game_id]

                        # Index 0 = home, 1 = away (matches games columns).
                        side = 0 if row['IS_HOME'] else 1
                        game[6 + side] = team_id
                        game[14 + side] = row['PTS']

                        out['team_game_stats'].append((
                            game_id, team_id,
                            row['MATCHUP'], row['WL'], row['IS_HOME'],
                            *stats, log_snapshot_id,
                        ))

                    else:  # mode == 'P'
                        player_id = int(row['PLAYER_ID'])
                        prior = players.get(player_id, (None, None))
                        players[player_id] = (
                            player_id,
                            row.get('PLAYER_NAME') or prior[1],
                        )
                        out['player_game_stats'].append((
                            game_id, team_id, player_id,
                            row['MATCHUP'], row['WL'],
                            *stats, log_snapshot_id,
                        ))

            # Preserve upcoming scheduled games (status 1 or 2) that have
            # no verified result yet. Final games missing results are not
            # invented here — the manifest should have failed validation
            # before we get here.
            for game_id, game in schedule.items():
                if (not game_id.startswith(prefix)
                        or str(game['game_status']) not in ('1', '2')
                        or not game['home_team_id']
                        or not game['away_team_id']):
                    continue
                for team_id in (game['home_team_id'], game['away_team_id']):
                    teams.setdefault(team_id, (team_id, None, None))
                games.setdefault(game_id, [
                    game_id, season, kind,
                    None,                              # game_date unknown yet
                    game['scheduled_tipoff_utc'],
                    game['game_status'],
                    game['home_team_id'], game['away_team_id'],
                    game['neutral_site'],
                    game['arena'], game['city'], game['state'], game['country'],
                    game['source'],
                    None, None,                        # scores
                    schedule_snapshot_id,
                ])

    out.update(
        snapshots=list(sources.values()),
        teams=list(teams.values()),
        players=list(players.values()),
        games=list(games.values()),
    )

    # Refuse to write an empty stats database.
    if not out['team_game_stats']:
        raise ValueError('No verified team statistics to load')

    return out


def build(manifest, database, *, replace=False, overrides=None):
    """Build (or rebuild) the DuckDB database from a prepared manifest.

    Writes to a temporary file and renames it into place, so a failed
    build never leaves a half-written database on disk.

    Args:
        manifest: Path to the collection manifest.
        database: Path to the target .duckdb file.
        replace: If True, overwrite an existing database.
        overrides: Optional venue overrides forwarded to prepare().

    Returns:
        A dict mapping each table name to the number of rows written.
    """
    import duckdb
    import pandas as pd

    database = Path(database)
    if database.exists() and not replace:
        raise FileExistsError(
            f'{database} exists; use --replace for an explicit rebuild'
        )

    data = prepare(manifest, overrides)

    database.parent.mkdir(parents=True, exist_ok=True)
    tmp = database.with_name(database.name + '.' + uuid4().hex + '.tmp')

    try:
        con = duckdb.connect(str(tmp))
        try:
            con.execute('BEGIN TRANSACTION')
            con.execute(SCHEMA)

            # Load each table in FK order, in batches, so large tables
            # don't blow up pandas memory.
            for table in TABLE_ORDER:
                rows = data[table]
                if not rows:
                    continue

                # Column order from the actual table, so we don't have to
                # repeat the schema here.
                columns = [
                    r[1]
                    for r in con.execute(
                        f"PRAGMA table_info('{table}')"
                    ).fetchall()
                ]

                for offset in range(0, len(rows), 50000):
                    frame = pd.DataFrame(
                        rows[offset:offset + 50000],
                        columns=columns,
                        dtype=object,
                    )
                    con.register('incoming_batch', frame)
                    con.execute(f'INSERT INTO {table} SELECT * FROM incoming_batch')
                    con.unregister('incoming_batch')

            con.execute(
                'INSERT INTO build_metadata VALUES (?,?,?)',
                ['0.4.0', datetime.now(timezone.utc), str(manifest)],
            )
            con.execute('COMMIT')
        finally:
            con.close()

        # Re-check the target didn't appear while we were building.
        if database.exists() and not replace:
            raise FileExistsError(database)
        tmp.replace(database)
    finally:
        # Clean up the temp database and any WAL it left behind.
        tmp.unlink(missing_ok=True)
        Path(str(tmp) + '.wal').unlink(missing_ok=True)

    return {k: len(v) for k, v in data.items()}


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--manifest', required=True, type=Path,
        help='Path to a completed collection manifest.',
    )
    parser.add_argument(
        '--database', type=Path, default=Path('data/database/nba.duckdb'),
        help='Target DuckDB file (default: %(default)s).',
    )
    parser.add_argument(
        '--replace', action='store_true',
        help='Overwrite an existing database.',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Validate the manifest and report row counts without writing.',
    )
    parser.add_argument(
        '--venue-overrides', type=Path,
        help='Path to a JSON file of venue overrides.',
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    try:
        overrides = (
            json.loads(args.venue_overrides.read_text(encoding='utf-8'))
            if args.venue_overrides else None
        )
        if args.dry_run:
            counts = {
                k: len(v) for k, v in prepare(args.manifest, overrides).items()
            }
        else:
            counts = build(
                args.manifest, args.database,
                replace=args.replace, overrides=overrides,
            )
        print(json.dumps(counts, indent=2))
    except Exception as exc:
        print(f'Build failed: {exc}')
        return EXIT_BUILD_FAILED
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
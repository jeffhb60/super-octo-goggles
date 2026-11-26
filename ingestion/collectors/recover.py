"""Revalidate failed season/type groups from exact manifest snapshots, offline.

Reads an existing collection manifest, re-runs validation against the
snapshots already referenced by failed or partial tasks (using the same
schedule snapshots the original run used), and writes a new manifest with
updated task statuses. Never touches the network, never modifies the
source manifest.

Tasks that can't be revalidated (missing team reference, request mismatch,
schedule reference mismatch, unreadable snapshot) are left as failed with
an updated error message.
"""
import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from ingestion.clients.cache import atomic_json, read_snapshot
from ingestion.clients.nba import NBAClient
from ingestion.clients.schedule import parse_schedule
from ingestion.collectors.historical import coverage


# Exit code used when any task remains failed or partial after recovery.
EXIT_NOT_CLEAN = 2

# Season types that can be recovered; anything else is skipped.
RECOVERABLE_KINDS = ('Regular Season', 'Playoffs')


def _expected_log_params(season, kind, mode):
    """Return the request params a full-season snapshot must carry."""
    return dict(
        season=season,
        season_type_all_star=kind,
        player_or_team_abbreviation=mode,
        league_id='00',
        date_from_nullable='',
        date_to_nullable='',
    )


def recover(source, destination):
    """Revalidate failed/partial season/type groups into a new manifest.

    Args:
        source: Path to the input manifest.
        destination: Path to write the recovered manifest to. Must not
            exist and must differ from `source`.

    Returns:
        The recovered manifest dict.
    """
    source = Path(source)
    destination = Path(destination)

    # Refuse to overwrite the source or any existing manifest.
    if (source.resolve() == destination.resolve()
            or destination.exists()):
        raise ValueError('Choose a new output manifest; originals are preserved')

    report = json.loads(source.read_text(encoding='utf-8'))
    if not report.get('finished_at'):
        raise ValueError('Source manifest unfinished')

    tasks = report['tasks']

    # No two tasks may share the same (season, dataset) key.
    keys = [(t['season'], t['dataset']) for t in tasks]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate tasks')

    # Only these season/type groups need revalidation.
    bad_groups = {
        (t['season'], t.get('season_type'))
        for t in tasks
        if t['status'] in ('failed', 'partial')
    }

    client = NBAClient()

    for season, kind in sorted(bad_groups, key=str):
        if kind not in RECOVERABLE_KINDS:
            continue

        # Tasks for this (season, kind), keyed by mode ('T' / 'P').
        group = {
            t['mode']: t
            for t in tasks
            if t['season'] == season and t.get('season_type') == kind
        }

        # Exactly one retrieved schedule snapshot must exist for the season.
        schedules = [
            t for t in tasks
            if (t['season'] == season
                and t['dataset'] == 'schedule'
                and t['status'] == 'retrieved')
        ]
        if len(schedules) != 1 or 'T' not in group:
            continue

        schedule_path = Path(schedules[0]['snapshot'])
        params, _, raw = read_snapshot(schedule_path)
        if params != {'Season': season, 'LeagueID': '00'}:
            raise ValueError('Schedule request mismatch')

        schedule = parse_schedule(raw, season)
        client.schedule = schedule

        # Team rows are needed as a reference for player-mode coverage.
        team_rows = None

        for mode in ('T', 'P'):
            if mode not in group:
                continue

            task = group[mode]
            if task['status'] == 'not_started':
                continue

            try:
                # The task must reference the same schedule snapshot we
                # just loaded.
                if (Path(task['schedule_snapshot']).resolve()
                        != schedule_path.resolve()):
                    raise ValueError('Schedule reference mismatch')

                # The task's log snapshot must be a full-season snapshot
                # for this exact request.
                params, _, raw = read_snapshot(Path(task['snapshot']))
                if params != _expected_log_params(season, kind, mode):
                    raise ValueError('Snapshot request mismatch')

                # Re-run validation and coverage against the snapshot.
                rows = client._validate(raw, mode, None, None, season)
                check = coverage(
                    rows, schedule, kind, client.issues,
                    team_rows if mode == 'P' else None,
                )

                # Player rows need team rows as a reference; without them,
                # mark the task partial instead of trusting the result.
                if mode == 'P' and team_rows is None:
                    check.update(
                        status='partial',
                        reason='Team reference unavailable',
                    )

                # Remember the team rows if this task verified cleanly, so
                # the player task can be checked against them.
                if mode == 'T' and check['status'] == 'verified':
                    team_rows = rows

                # Replace any previous failure state with the new result.
                task.pop('error', None)
                task.pop('reason', None)
                task.update(
                    check,
                    excluded_games=client.excluded,
                    revalidated_offline=True,
                )
                # Preserve the old quality report path for reference; the
                # new result may not have produced one.
                task['previous_quality_report'] = task.pop(
                    'quality_report', None
                )

            except Exception as exc:
                task.update(status='failed', error=str(exc))

            print(season, kind, mode, task['status'])

    # Rewrite the top-level metadata to reflect the recovery.
    report.update(
        version='0.4.1',
        recovered_from=str(source),
        recovered_at=datetime.now(timezone.utc).isoformat(),
        counts=dict(Counter(t['status'] for t in tasks)),
    )
    atomic_json(destination, report)
    return report


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--manifest', required=True,
        help='Input manifest to revalidate.',
    )
    parser.add_argument(
        '--output', required=True,
        help='New manifest to write; must not already exist.',
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    report = recover(args.manifest, args.output)
    print(json.dumps(report['counts'], indent=2))

    # Exit non-zero if any task is still failed or partial after recovery.
    return (
        EXIT_NOT_CLEAN
        if any(t['status'] in ('failed', 'partial') for t in report['tasks'])
        else 0
    )


if __name__ == '__main__':
    raise SystemExit(main())
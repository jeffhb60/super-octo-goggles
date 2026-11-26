"""Copy legacy snapshots into a readable layout; originals are never deleted.

Legacy snapshots live under `leaguegamelog/<24-hex-hash>/` with opaque
filenames. This tool re-saves each one into the current `location()` layout
(which derives a readable path from the request params) and, if a matching
`.quality.json` sidecar exists, copies that alongside too.

By default it runs as a dry run and only prints what it would do. Pass
`--apply` to actually write the copies. The originals are always retained.
"""
import argparse
import json
import re
from pathlib import Path

from ingestion.clients.cache import read_snapshot, save_snapshot, atomic_json, location

# Legacy cache folders are named with a 24-character lowercase hex digest.
LEGACY_FOLDER_PATTERN = re.compile(r'[0-9a-f]{24}')

def migrate(root, apply=False):
    """Copy legacy snapshots under `root` into the readable layout.

    Args:
        root: Cache root directory (e.g. `data/raw/api`).
        apply: If True, write the copies. If False, only report what would
            happen (dry run).

    Returns:
        The number of errors encountered. Zero means the run succeeded.
    """
    total = 0
    errors = 0

    # Legacy layout: leaguegamelog/<hex-hash>/*.json
    for folder in sorted((root / 'leaguegamelog').glob('*')):
        if not folder.is_dir() or not LEGACY_FOLDER_PATTERN.fullmatch(folder.name):
            continue

        for path in sorted(folder.glob('*.json')):
            # Skip quality sidecars; they're handled separately below.
            if path.name.endswith('.quality.json'):
                continue

            try:
                params, stamp, raw = read_snapshot(path)

                # Compute the destination path in the new layout so we can
                # report it even in dry-run mode.
                dest, prefix = location(root, params)
                new_name = prefix + '_' + stamp.strftime('%Y%m%dT%H%M%S%fZ') + '.json'
                print(f'{path.name} -> {dest / new_name}')

                if apply:
                    # Re-save via the current helper so the new layout is
                    # produced exactly as a live fetch would.
                    new = save_snapshot(root, params, stamp, raw)

                    # If the legacy snapshot had a quality report, copy it
                    # over too — but never overwrite one that already exists.
                    old_report = Path(str(path) + '.quality.json')
                    new_report = Path(str(new) + '.quality.json')
                    if old_report.exists() and not new_report.exists():
                        report = json.loads(old_report.read_text(encoding='utf-8'))
                        report['snapshot'] = str(new)
                        atomic_json(new_report, report)

                total += 1

            except (ValueError, TypeError, KeyError, OSError) as exc:
                # A single bad snapshot shouldn't abort the whole migration;
                # record it and keep going.
                errors += 1
                print(f'ERROR {path}: {exc}')

    action = 'Copied/verified' if apply else 'Would copy'
    print(f'{action}: {total}; errors: {errors}. Originals retained.')
    return errors

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--cache-dir',
        type=Path,
        default=Path('data/raw/api'),
        help='Cache root directory (default: %(default)s).',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Write copies; default is a dry run.',
    )
    args = parser.parse_args()

    # Exit non-zero if any snapshot failed to migrate.
    raise SystemExit(bool(migrate(args.cache_dir, args.apply)))
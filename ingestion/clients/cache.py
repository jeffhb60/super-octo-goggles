"""Readable immutable snapshots; legacy cache migration without network access.

Snapshots are stored in a human-readable path derived from the request params:

    <root>/leaguegamelog/<season>/<season_type>/<entity>/
        full_season_<timestamp>.json                       (no date filter)
        incremental/<start>_to_<end>_<timestamp>.json      (date filter)

Each snapshot records a metadata block with hashes of both the request and
the response, so corruption and tampering can be detected on read.

This module also understands the older layout, where snapshots lived under
`<root>/leaguegamelog/<24-hex-hash>/`. Nothing here touches the network.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


# Legacy cache folders are named with a 24-character lowercase hex digest.
LEGACY_FOLDER_PATTERN = re.compile(r'[0-9a-f]{24}')

# Regex for a valid season string like "2023-24".
SEASON_PATTERN = re.compile(r'\d{4}-\d{2}')

# Map API mode letters to readable entity names used in paths.
ENTITY_BY_MODE = {'T': 'teams', 'P': 'players'}
ENTITY_TYPE_BY_MODE = {'T': 'team', 'P': 'player'}


def digest(value):
    """Return a stable SHA-256 hex digest of a JSON-serializable value."""
    # Canonical serialization: sorted keys, no extra whitespace, keep unicode.
    canonical = json.dumps(
        value, sort_keys=True, separators=(',', ':'), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def legacy_key(params):
    """Compute the folder name used by the legacy cache layout."""
    return hashlib.sha256(
        json.dumps(params, sort_keys=True).encode()
    ).hexdigest()[:24]


def utc(value):
    """Parse an ISO-8601 timestamp and normalize it to UTC.

    Rejects naive timestamps so timezone ambiguity can't silently skew
    ordering or filenames.
    """
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timestamp must contain timezone')
    return parsed.astimezone(timezone.utc)


def location(root, params):
    """Return (folder, filename_prefix) for a snapshot in the readable layout.

    The prefix is either "full_season" (no date filter) or
    "<start>_to_<end>" (with date filter); open-ended ranges use "open".
    """
    season = params['season']
    if not SEASON_PATTERN.fullmatch(season):
        raise ValueError('Invalid season')

    # Normalize the season type to a lowercase, underscore-separated token.
    kind = re.sub(
        r'[^a-z0-9]+', '_', params['season_type_all_star'].lower()
    ).strip('_')
    if not kind:
        raise ValueError('Invalid season type')

    entity = ENTITY_BY_MODE[params['player_or_team_abbreviation']]
    folder = Path(root) / 'leaguegamelog' / season / kind / entity

    start = params['date_from_nullable']
    end = params['date_to_nullable']

    def format_bound(value):
        """Format an MM/DD/YYYY bound as YYYYMMDD, or "open" if absent."""
        if not value:
            return 'open'
        return datetime.strptime(value, '%m/%d/%Y').strftime('%Y%m%d')

    # Only use the "incremental" subfolder when at least one bound is set.
    if start or end:
        return folder / 'incremental', f'{format_bound(start)}_to_{format_bound(end)}'

    return folder, 'full_season'


def atomic_json(path, obj):
    """Write `obj` as pretty JSON to `path` atomically.

    Writes to a temporary file in the same directory, then renames it into
    place, so readers never see a partial file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
    try:
        tmp.write_text(
            json.dumps(obj, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        tmp.replace(path)
    finally:
        # Ensure the temp file is cleaned up on failure.
        tmp.unlink(missing_ok=True)


def read_snapshot(path):
    """Load a snapshot and return (params, fetched_at_utc, response).

    Handles both the current format (with a `metadata` block containing
    request/response hashes) and the legacy format (params/fetched_at/response
    at the top level). Response bodies are decoded from JSON when they were
    stored as strings.
    """
    obj = json.loads(path.read_text(encoding='utf-8'))

    if 'metadata' in obj:
        # Current format: verify both hashes before trusting the content.
        meta = obj['metadata']
        params = meta['params']
        stamp = meta['fetched_at']
        response = obj['response']
        if (digest(params) != meta['request_hash']
                or digest(response) != meta['response_hash']):
            raise ValueError('Snapshot hash mismatch')
    else:
        # Legacy format: no hashes to verify, just pull fields out.
        params = obj['params']
        stamp = obj['fetched_at']
        response = obj['response']
        if isinstance(response, str):
            try:
                response = json.loads(response)
            except ValueError:
                pass

    return params, utc(stamp), response


def save_snapshot(root, params, stamp, raw):
    """Persist a snapshot in the readable layout and return its path.

    If an identical snapshot already exists at the computed path, the
    existing path is returned unchanged. Otherwise a short UUID suffix is
    appended so the new content is preserved without overwriting.
    """
    # Decode JSON-encoded bodies when possible, but keep malformed upstream
    # payloads as-is so they can still be inspected.
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            pass

    folder, prefix = location(root, params)
    stamp = stamp.astimezone(timezone.utc)
    path = folder / f'{prefix}_{stamp:%Y%m%dT%H%M%S%fZ}.json'

    meta = {
        'endpoint': 'leaguegamelog',
        'season': params['season'],
        'season_type': params['season_type_all_star'],
        'entity_type': ENTITY_TYPE_BY_MODE[params['player_or_team_abbreviation']],
        'extraction_type': (
            'full_season' if prefix == 'full_season' else 'date_range'
        ),
        'fetched_at': stamp.isoformat(),
        'request_hash': digest(params),
        'response_hash': digest(raw),
        'schema_version': '1.0',
        'params': params,
    }
    obj = {'metadata': meta, 'response': raw}

    # If the exact same snapshot already exists, reuse it.
    if path.exists():
        if json.loads(path.read_text(encoding='utf-8')) == obj:
            return path
        # Otherwise, disambiguate with a short UUID rather than overwrite.
        path = path.with_name(path.stem + '_' + uuid4().hex[:8] + '.json')

    atomic_json(path, obj)
    return path


def candidates(root, params):
    """Return all snapshots for `params`, newest first.

    Searches both the readable layout and the legacy layout, skipping
    `.quality.json` sidecars and unreadable or mismatched files.
    """
    folder, prefix = location(root, params)
    legacy_folder = Path(root) / 'leaguegamelog' / legacy_key(params)

    found = []
    paths = (
        list(folder.glob(prefix + '*.json'))
        + list(legacy_folder.glob('*.json'))
    )
    for path in paths:
        if path.name.endswith('.quality.json'):
            continue
        try:
            actual_params, stamp, raw = read_snapshot(path)
            if actual_params == params:
                found.append((stamp, path, raw))
        except (ValueError, KeyError, TypeError, OSError):
            # Skip anything unreadable or corrupt.
            continue

    # Sort by timestamp descending so callers get the newest snapshot first.
    return sorted(found, key=lambda entry: entry[0], reverse=True)
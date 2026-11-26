"""Parse NBA season schedule snapshots without guessing venue designations.

The parser is deliberately strict: it validates the season and league,
rejects malformed or duplicate game IDs, and only preserves `neutral_site`
when the upstream value is a real boolean. Anything ambiguous is left as
None rather than guessed, so downstream code can tell "not neutral" apart
from "unknown".
"""
import re
from datetime import datetime, timezone


# NBA game IDs are 10-digit strings (e.g. "0022200001").
GAME_ID_PATTERN = re.compile(r'\d{10}')

# The only league ID we accept in a schedule (00 = NBA).
EXPECTED_LEAGUE_ID = '00'

# Timestamps before this year are treated as bogus and dropped.
MIN_PLAUSIBLE_YEAR = 1900


def parse_schedule(raw, season):
    """Parse a ScheduleLeagueV2 payload into a dict keyed by game ID.

    Args:
        raw: The decoded JSON schedule payload.
        season: Expected season in YYYY-YY form (e.g. "2022-23").

    Returns:
        A dict mapping game_id -> dict with normalized fields
        (home/away team IDs, neutral_site, arena/location, tip-off, etc.).

    Raises:
        ValueError: If the season/league don't match, a game ID is invalid
            or duplicated, or the schedule contains no games.
    """
    league = raw['leagueSchedule']

    # Verify we're parsing the season and league we expected.
    if league.get('seasonYear') != season:
        raise ValueError('Schedule season mismatch')
    if str(league.get('leagueId')) != EXPECTED_LEAGUE_ID:
        raise ValueError('Schedule league mismatch')

    result = {}

    for day in league['gameDates']:
        for game in day['games']:
            game_id = game['gameId']

            # Game IDs must be present, 10-digit strings, and unique.
            if (not isinstance(game_id, str)
                    or not GAME_ID_PATTERN.fullmatch(game_id)):
                raise ValueError('Invalid schedule game ID')
            if game_id in result:
                raise ValueError(f'Duplicate schedule game {game_id}')

            home_id = int(game['homeTeam']['teamId'])
            away_id = int(game['awayTeam']['teamId'])

            # `isNeutral` is only trusted when it's an actual bool; any other
            # value (missing, null, string, etc.) becomes None to signal
            # "unknown" rather than being coerced.
            neutral = game.get('isNeutral')
            if type(neutral) is not bool:
                neutral = None

            tip = _parse_tipoff(game.get('gameDateTimeUTC'))

            result[game_id] = dict(
                game_id=game_id,
                home_team_id=home_id or None,
                away_team_id=away_id or None,
                neutral_site=neutral,
                arena=game.get('arenaName'),
                city=game.get('arenaCity'),
                state=game.get('arenaState'),
                country=game.get('arenaCountry'),
                scheduled_tipoff_utc=tip,
                game_status=game.get('gameStatus'),
                source='ScheduleLeagueV2',
            )

    if not result:
        raise ValueError('Empty schedule')

    return result


def _parse_tipoff(value):
    """Parse a UTC tip-off timestamp, or return None if it isn't usable.

    Accepts ISO-8601 strings (with "Z" or an explicit offset). Naive
    timestamps and implausibly old years are treated as missing.
    """
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

    # A naive timestamp can't be safely normalized to UTC.
    if parsed.tzinfo is None:
        return None

    # Reject obviously bogus dates (e.g. year 0001 sentinels).
    if parsed.year < MIN_PLAUSIBLE_YEAR:
        return None

    return parsed.astimezone(timezone.utc).isoformat()
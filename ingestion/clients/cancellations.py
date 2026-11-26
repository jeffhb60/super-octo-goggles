"""Documented historical cancellations; never infer cancellation from zero scores.

Cancellations must be explicitly recorded in `CANCELED` — this module never
guesses that a game was cancelled just because its box score is all zeros.
A game is treated as a documented cancellation only when:

1. Its ID is in `CANCELED` for the requested season.
2. The schedule doesn't show it as final, or the schedule entry names the
   same teams as the cancellation record.
3. The fetched pair has the expected two teams, the expected date, and an
   all-zero placeholder row for both teams.
"""
CANCELED = {
    '0021201214': (
        '2012-13',
        '2013-04-16',
        {1610612754, 1610612738},
    ),
}

# Stat columns that must all be zero on a placeholder row. Note this is
# narrower than the full stats set: WL and PTS are checked separately.
ZERO_STAT_COLUMNS = (
    'MIN FGM FGA FG3M FG3A FTM FTA '
    'OREB DREB REB AST STL BLK TOV PF PTS PLUS_MINUS'
).split()


def confirmed(game_id, season, schedule):
    """Return True if `game_id` is a documented cancellation in `season`.

    "Documented" means the game appears in `CANCELED` for the same season
    and, if the schedule has an entry for it, that entry isn't final (status
    != '3') and names the same teams as the cancellation record.
    """
    known = CANCELED.get(game_id)
    if not known or not schedule:
        return False

    known_season, _known_date, known_teams = known
    if known_season != season:
        return False

    scheduled = schedule.get(game_id)
    if scheduled is None:
        return True

    # If the schedule does list it, it must not be final and must name
    # the same teams as the cancellation record.
    if str(scheduled['game_status']) == '3':
        return False
    scheduled_teams = {
        scheduled['home_team_id'],
        scheduled['away_team_id'],
    }
    return scheduled_teams == known_teams


def placeholder(game_id, season, schedule, pair):
    """Return True if `pair` is the all-zero placeholder for a cancellation.

    Requires that:
    - `confirmed()` is true for this game and season.
    - `pair` has exactly the two teams from the cancellation record.
    - Both rows use the recorded cancellation date, have no win/loss
      result (`WL is None`), and have every zero-checkable stat at 0.
    """
    if not confirmed(game_id, season, schedule):
        return False

    _known_season, known_date, known_teams = CANCELED[game_id]

    # Both rows must have the expected teams.
    if len(pair) != 2:
        return False
    if {int(r['TEAM_ID']) for r in pair} != known_teams:
        return False

    # Both rows must look like placeholders: correct date, no result,
    # and all stat columns zero.
    for r in pair:
        if r['GAME_DATE'] != known_date:
            return False
        if r['WL'] is not None:
            return False
        if any(r[column] != 0 for column in ZERO_STAT_COLUMNS):
            return False

    return True
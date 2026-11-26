"""Unit tests for cancellation handling across collect, recover, and the loader.

Covers three related paths:

1. A documented cancellation is excluded from coverage, recovered offline,
   and preserved in the loaded database as a games row with no scores.
2. A cancellation that the schedule marks final (status 3) is rejected.
3. A zero-score row for a game that isn't a documented cancellation is
   rejected rather than silently accepted or dropped.
"""
import json
import tempfile
import unittest
from pathlib import Path

from ingestion.clients.nba import NBAClient, DataError
from ingestion.collectors.historical import collect
from ingestion.collectors.recover import recover
from ingestion.loaders.duckdb import prepare
from test_nba import fixture
from test_schedule import schedule_fixture


# The one game ID currently recorded as a documented cancellation, and the
# season it belongs to. Must match the entry in cancellations.CANCELED.
GID = '0021201214'
SEASON = '2012-13'

# Teams for the canceled game, in the order (away, home) they appear in
# the cancellation record.
AWAY_TEAM_ID = 1610612754
HOME_TEAM_ID = 1610612738


def inputs():
    """Return (game_log_fixture, schedule_fixture) for the cancellation tests.

    The game log contains the normal 2022-23 fixture rows re-dated to
    2012-13 (game '0021200001') plus a placeholder pair for the canceled
    game '0021201214' with all-zero stats and no win/loss result.

    The schedule contains the 2012-13 entry for the normal game and a
    non-final (status 1) entry for the canceled game.
    """
    data = fixture()
    result_set = data['resultSets'][0]
    headers = result_set['headers']

    # Re-date the normal fixture rows to the 2012-13 season.
    for row in result_set['rowSet']:
        row[headers.index('SEASON_ID')] = '22012'
        row[headers.index('GAME_ID')] = '0021200001'
        row[headers.index('GAME_DATE')] = '2012-10-30'

    # Append the canceled game's placeholder pair: all-zero stats, no result.
    for team_id, matchup in [
        (AWAY_TEAM_ID, 'IND @ BOS'),
        (HOME_TEAM_ID, 'BOS vs. IND'),
    ]:
        row = dict.fromkeys(headers, 0)
        row.update(
            SEASON_ID='22012',
            TEAM_ID=team_id,
            GAME_ID=GID,
            GAME_DATE='2013-04-16',
            MATCHUP=matchup,
            WL=None,
            FG_PCT=None, FG3_PCT=None, FT_PCT=None,
        )
        result_set['rowSet'].append([row[k] for k in headers])

    schedule = schedule_fixture()
    schedule['leagueSchedule']['seasonYear'] = SEASON
    games = schedule['leagueSchedule']['gameDates'][0]['games']

    # Re-date the normal scheduled game to match the re-dated fixture.
    games[0].update(
        gameId='0021200001',
        gameDateTimeUTC='2012-10-30T23:00:00Z',
    )

    # The canceled game is scheduled (status 1), not final, and never played.
    games.append(dict(
        gameId=GID,
        homeTeam={'teamId': HOME_TEAM_ID},
        awayTeam={'teamId': AWAY_TEAM_ID},
        gameStatus=1,
    ))

    return data, schedule


class CancellationTests(unittest.TestCase):
    def test_recovery_and_loader(self):
        """A failed/partial pair for the canceled game can be recovered
        offline and loaded as a scoreless games row."""
        data, schedule = inputs()

        def fetch(params, timeout):
            if params['player_or_team_abbreviation'] == 'T':
                return data

            # Player fixture: same rows, one PLAYER_ID column added.
            player_data = json.loads(json.dumps(data))
            result_set = player_data['resultSets'][0]
            result_set['headers'].append('PLAYER_ID')
            result_set['rowSet'] = [
                row + [100 + index]
                for index, row in enumerate(result_set['rowSet'][:2])
            ]
            return player_data

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            client = NBAClient(
                root / 'api',
                fetcher=fetch,
                schedule_fetcher=lambda p, t: schedule,
                min_interval=0,
            )

            old = root / 'old.json'
            new = root / 'new.json'

            # First collection: the canceled game should be recorded as
            # excluded (not quarantined) in the team task.
            report = collect(
                client, [SEASON], old,
                kinds=('Regular Season',),
                active_season='2026-27',
            )
            self.assertEqual(
                report['tasks'][1]['excluded_games'][0]['game_id'],
                GID,
            )

            # Simulate an earlier run where the canceled game broke
            # validation: the team task failed and the player task was
            # downgraded to partial for lack of a team reference.
            report['tasks'][1].update(
                status='failed', error='Incomplete result',
            )
            report['tasks'][2].update(
                status='partial', reason='Team reference unavailable',
            )
            old.write_text(json.dumps(report))
            before = old.read_bytes()

            # Recover offline and confirm the source manifest is untouched.
            fixed = recover(old, new)
            self.assertEqual(
                fixed['counts'],
                {'retrieved': 1, 'verified': 2},
            )
            self.assertEqual(old.read_bytes(), before)

            # The loader accepts the recovered manifest and preserves the
            # canceled game as a scoreless games row.
            result = prepare(new)
            self.assertEqual(len(result['team_game_stats']), 2)
            self.assertEqual(len(result['player_game_stats']), 2)

            game = next(g for g in result['games'] if g[0] == GID)
            # games columns 14:16 are (home_points, away_points).
            self.assertEqual(game[14:16], [None, None])

    def test_final_conflict_rejected(self):
        """If the schedule marks the canceled game final, cancellation
        handling refuses to treat it as a placeholder."""
        data, schedule = inputs()
        schedule['leagueSchedule']['gameDates'][0]['games'][1]['gameStatus'] = 3

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d,
                fetcher=lambda p, t: data,
                schedule_fetcher=lambda p, t: schedule,
                min_interval=0,
            )
            with self.assertRaises(DataError):
                client.game_logs(SEASON, resolve_schedule=True)

    def test_unknown_zero_record_rejected(self):
        """A zero-score pair for a game that isn't documented as canceled
        must be rejected — not silently accepted or excluded."""
        data, schedule = inputs()

        # Re-ID both placeholder rows to an undocumented game.
        rows = data['resultSets'][0]['rowSet']
        rows[-1][2] = '0021201215'
        rows[-2][2] = '0021201215'

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d,
                fetcher=lambda p, t: data,
                schedule_fetcher=lambda p, t: schedule,
                min_interval=0,
            )
            with self.assertRaises(DataError):
                client.game_logs(SEASON, resolve_schedule=True)
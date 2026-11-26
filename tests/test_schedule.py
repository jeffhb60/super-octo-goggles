"""Unit tests for season-schedule parsing and schedule-based game resolution."""
import json
import tempfile
import unittest

from ingestion.clients.nba import NBAClient, DataError
from ingestion.clients.schedule import parse_schedule
from test_nba import fixture


def schedule_fixture():
    """Build a minimal schedule payload for a single neutral-site game.

    The game matches the one produced by `fixture()` (game '0022200001',
    teams 1 and 2) but declares team 2 as home and marks it as neutral-site,
    which lets tests verify that the schedule overrides MATCHUP.
    """
    return {
        'leagueSchedule': {
            'seasonYear': '2022-23',
            'leagueId': '00',
            'gameDates': [{
                'games': [{
                    'gameId': '0022200001',
                    'homeTeam': {'teamId': 2},
                    'awayTeam': {'teamId': 1},
                    'isNeutral': True,
                    'arenaName': 'Test Arena',
                    'gameDateTimeUTC': '2022-10-19T00:00:00Z',
                    'gameStatus': 3,
                }],
            }],
        },
    }


class ScheduleTests(unittest.TestCase):
    def test_resolution_and_cache(self):
        """The schedule overrides ambiguous home/away, annotates arena, and
        is cached so a second resolve_schedule call makes no extra request."""
        f = fixture()

        # Corrupt MATCHUP so both rows look like away games; only the
        # schedule can disambiguate home/away.
        f['resultSets'][0]['rowSet'][0][4] = 'A @ B'

        calls = []

        def schedule_fetcher(params, timeout):
            calls.append(params)
            return json.dumps(schedule_fixture())

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d,
                min_interval=0,
                fetcher=lambda p, t: json.dumps(f),
                schedule_fetcher=schedule_fetcher,
            )

            rows = client.game_logs('2022-23', resolve_schedule=True)

            # Home/away comes from the schedule, not MATCHUP.
            self.assertEqual([r['IS_HOME'] for r in rows], [False, True])
            self.assertTrue(all(r['NEUTRAL_SITE'] for r in rows))
            self.assertEqual(rows[0]['ARENA'], 'Test Arena')

            # The schedule snapshot is persisted.
            self.assertTrue(client.schedule_snapshot_path.exists())

            # A second resolve_schedule call reuses the cached schedule
            # instead of fetching it again.
            client.game_logs('2022-23', reuse_cache=True, resolve_schedule=True)
            self.assertEqual(len(calls), 1)

    def test_conflicting_teams(self):
        """If the schedule names a home team that isn't in the fetched rows,
        resolution fails with DataError."""
        schedule = schedule_fixture()
        schedule['leagueSchedule']['gameDates'][0]['games'][0]['homeTeam']['teamId'] = 3

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d,
                min_interval=0,
                fetcher=lambda p, t: fixture(),
                schedule_fetcher=lambda p, t: schedule,
            )
            with self.assertRaises(DataError):
                client.game_logs('2022-23', resolve_schedule=True)

    def test_wrong_season(self):
        """Parsing a schedule whose seasonYear doesn't match the requested
        season is rejected."""
        with self.assertRaises(ValueError):
            parse_schedule(schedule_fixture(), '2023-24')

    def test_unknown_neutral(self):
        """A game with no isNeutral flag parses with neutral_site=None rather
        than being rejected or defaulting to True/False."""
        schedule = schedule_fixture()
        del schedule['leagueSchedule']['gameDates'][0]['games'][0]['isNeutral']

        parsed = parse_schedule(schedule, '2022-23')
        self.assertIsNone(parsed['0022200001']['neutral_site'])
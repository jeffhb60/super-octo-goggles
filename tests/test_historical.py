"""Unit tests for the historical season collector and coverage reporting."""
import json
import tempfile
import unittest
from pathlib import Path

from ingestion.clients.nba import NBAClient
from ingestion.collectors.historical import collect, season_range, coverage
from test_nba import fixture
from test_schedule import schedule_fixture


def player_fixture():
    """Return a player-mode fixture with a PLAYER_ID column and per-row IDs.

    The shared `fixture()` produces team-mode rows; player mode requires an
    extra PLAYER_ID column. This helper adds it and gives each row a distinct
    player ID so player-mode results look realistic.
    """
    f = fixture()
    result_set = f['resultSets'][0]
    result_set['headers'] = result_set['headers'] + ['PLAYER_ID']
    result_set['rowSet'] = [
        row + [index + 100]
        for index, row in enumerate(result_set['rowSet'])
    ]
    return f


class HistoricalTests(unittest.TestCase):
    def test_season_range(self):
        """season_range expands inclusive endpoints and rejects bad years."""
        self.assertEqual(
            season_range('1999-00', '2001-02'),
            ['1999-00', '2000-01', '2001-02'],
        )

        # "2022-24" is not a valid YYYY-YY season (years aren't consecutive).
        with self.assertRaises(ValueError):
            season_range('2022-24', '2023-24')

    def test_resume_and_single_schedule_request(self):
        """Two collect() runs against the same cache should hit the schedule
        only once and produce identical verified manifests."""
        calls = []

        def fetch(params, timeout):
            calls.append(params['player_or_team_abbreviation'])
            return json.dumps(player_fixture() if calls[-1] == 'P' else fixture())

        def schedule_fetch(params, timeout):
            calls.append('schedule')
            return schedule_fixture()

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            client = NBAClient(
                root / 'api',
                min_interval=0,
                fetcher=fetch,
                schedule_fetcher=schedule_fetch,
                allow_partial=True,
            )

            for n in range(2):
                report = collect(
                    client,
                    ['2022-23'],
                    root / f'{n}.json',
                    kinds=('Regular Season',),
                    active_season='2026-27',
                )
                self.assertEqual(
                    report['counts'],
                    {'retrieved': 1, 'verified': 2},
                )
                # The manifest on disk must match the returned report.
                on_disk = json.loads((root / f'{n}.json').read_text())
                self.assertEqual(on_disk['counts'], report['counts'])

            # Schedule fetched once; each mode fetched once. The second
            # collect() run reuses the cache.
            self.assertEqual(calls, ['schedule', 'T', 'P'])

    def test_coverage_missing_game(self):
        """A game present in the schedule but absent from rows is reported
        as missing and downgrades status to partial."""
        schedule = {
            '0022200001': dict(game_status=3, home_team_id=1, away_team_id=2),
            '0022200002': dict(game_status=3, home_team_id=1, away_team_id=2),
        }
        rows = [
            dict(GAME_ID='0022200001', TEAM_ID=team_id)
            for team_id in (1, 2)
        ]

        result = coverage(rows, schedule, 'Regular Season', [])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['missing_game_ids'], ['0022200002'])

    def test_failure_isolated_and_current_refreshed(self):
        """A team-mode failure should not abort the whole run: the player
        task still runs, the manifest records the failure, and current-season
        runs never reuse cache."""
        calls = []

        def fetch(params, timeout):
            calls.append(params['player_or_team_abbreviation'])
            if calls[-1] == 'T':
                raise RuntimeError('simulated failure')
            return json.dumps(player_fixture())

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            client = NBAClient(
                root / 'api',
                min_interval=0,
                attempts=1,
                fetcher=fetch,
                schedule_fetcher=lambda p, t: schedule_fixture(),
                allow_partial=True,
            )

            for i in range(2):
                report = collect(
                    client,
                    ['2022-23'],
                    root / f'{i}.json',
                    active_season='2022-23',
                )
                self.assertEqual(
                    report['counts'],
                    {'retrieved': 1, 'failed': 1, 'partial': 1, 'not_started': 2},
                )

            # Because the season is "current", both runs actually fetch:
            # T then P each time.
            self.assertEqual(calls, ['T', 'P', 'T', 'P'])

    def test_player_team_pair_mismatch(self):
        """Player rows that don't line up with team rows (a missing team
        here) downgrade coverage to partial."""
        schedule = {
            '0022200001': dict(game_status=3, home_team_id=1, away_team_id=2),
        }
        rows = [dict(GAME_ID='0022200001', TEAM_ID=1)]
        teams = rows + [dict(GAME_ID='0022200001', TEAM_ID=2)]

        self.assertEqual(
            coverage(rows, schedule, 'Regular Season', [], teams)['status'],
            'partial',
        )
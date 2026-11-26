"""Unit tests for the offline DuckDB loader: manifest validation and build."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from ingestion.clients.nba import NBAClient
from ingestion.collectors.historical import collect
from ingestion.loaders.duckdb import prepare, build
from test_nba import fixture
from test_schedule import schedule_fixture


def make_manifest(root):
    """Run a full collect() against fake fetchers and return the manifest path.

    Produces a completed manifest for the 2022-23 regular season with both
    team and player datasets, using in-memory fixtures instead of the
    network. This is the standard input for the loader tests below.
    """
    def fetch(params, timeout):
        data = fixture()
        if params['player_or_team_abbreviation'] == 'P':
            # Player mode needs an extra PLAYER_ID column; assign per-row IDs.
            result_set = data['resultSets'][0]
            result_set['headers'] = result_set['headers'] + ['PLAYER_ID']
            result_set['rowSet'] = [
                row + [100 + index]
                for index, row in enumerate(result_set['rowSet'])
            ]
        return data

    client = NBAClient(
        root / 'api',
        fetcher=fetch,
        schedule_fetcher=lambda p, t: schedule_fixture(),
        min_interval=0,
    )

    manifest = root / 'manifest.json'
    collect(
        client,
        ['2022-23'],
        manifest,
        kinds=('Regular Season',),
        # Pretend we're in a much later season, so cache reuse is allowed
        # and no "not started" short-circuit applies.
        active_season='2026-27',
    )
    return manifest


class LoaderTests(unittest.TestCase):
    def test_prepare(self):
        """prepare() loads both stats tables, records all three snapshots,
        and derives home/away scores from the schedule."""
        with tempfile.TemporaryDirectory() as d:
            data = prepare(make_manifest(Path(d)))

            self.assertEqual(len(data['team_game_stats']), 2)
            self.assertEqual(len(data['player_game_stats']), 2)
            self.assertEqual(len(data['snapshots']), 3)

            # games columns 14:16 are (home_points, away_points).
            # The schedule designates team 2 as home, so home=90, away=100.
            game = next(g for g in data['games'] if g[0] == '0022200001')
            self.assertEqual(game[14:16], [90, 100])

    def test_reject_partial(self):
        """A manifest containing a 'partial' task must be rejected."""
        with tempfile.TemporaryDirectory() as d:
            path = make_manifest(Path(d))

            # Corrupt one task's status to 'partial'.
            obj = json.loads(path.read_text())
            obj['tasks'][1]['status'] = 'partial'
            path.write_text(json.dumps(obj))

            with self.assertRaises(ValueError):
                prepare(path)

    def test_reject_tampered_snapshot(self):
        """A snapshot whose response payload doesn't match its recorded
        hash must be rejected."""
        with tempfile.TemporaryDirectory() as d:
            path = make_manifest(Path(d))

            # Tamper with a row inside a referenced snapshot.
            obj = json.loads(path.read_text())
            snapshot_path = Path(obj['tasks'][1]['snapshot'])
            payload = json.loads(snapshot_path.read_text())
            payload['response']['resultSets'][0]['rowSet'][0][0] = 'bad'
            snapshot_path.write_text(json.dumps(payload))

            with self.assertRaises(ValueError):
                prepare(path)

    @unittest.skipUnless(
        importlib.util.find_spec('duckdb') and importlib.util.find_spec('pandas'),
        'DuckDB and pandas required',
    )
    def test_build_and_replace(self):
        """Building a database writes the expected rows and the game_results
        view; an existing database is refused unless replace=True."""
        import duckdb

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            manifest = make_manifest(root)
            db = root / 'nba.duckdb'

            # First build creates the database.
            build(manifest, db)

            # A second build without --replace is refused.
            with self.assertRaises(FileExistsError):
                build(manifest, db)

            # --replace rebuilds successfully.
            build(manifest, db, replace=True)

            con = duckdb.connect(str(db), read_only=True)
            try:
                self.assertEqual(
                    con.execute('select count(*) from team_game_stats').fetchone()[0],
                    2,
                )
                # Fixture game: home team (2) scored 90, away (1) scored 100,
                # so home_win=0 and mov=-10.
                self.assertEqual(
                    con.execute(
                        "select home_win, mov from game_results "
                        "where game_id='0022200001'"
                    ).fetchone(),
                    (0, -10),
                )
            finally:
                con.close()
"""Unit tests for the NBA Stats client and the cache migration helper."""
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.clients.cache import legacy_key, read_snapshot
from ingestion.clients.nba import NBAClient, DataError
from migrate_cache import migrate


# Columns the client expects in every LeagueGameLog response.
FIELDS = (
    'SEASON_ID TEAM_ID GAME_ID GAME_DATE MATCHUP WL MIN '
    'FGM FGA FG_PCT FG3M FG3A FG3_PCT FTM FTA FT_PCT '
    'OREB DREB REB AST STL BLK TOV PF PTS PLUS_MINUS'
).split()


def fixture():
    """Build a minimal, valid LeagueGameLog payload for a single game.

    Produces the two team rows for game '0022200001': team 1 wins at home
    (100-90), team 2 loses on the road. Both rows share the same date.
    """
    rows = []
    for team, matchup, win, pts in [
        (1, 'A vs. B', 'W', 100),
        (2, 'B @ A', 'L', 90),
    ]:
        # Start with all columns set to 1, then override the ones we care about.
        r = dict.fromkeys(FIELDS, 1)
        r.update(
            SEASON_ID='22022',
            TEAM_ID=team,
            GAME_ID='0022200001',
            GAME_DATE='2022-10-18',
            MATCHUP=matchup,
            WL=win,
            PTS=pts,
            # PLUS_MINUS must equal the point differential.
            PLUS_MINUS=10 if win == 'W' else -10,
        )
        rows.append([r[k] for k in FIELDS])

    return {
        'resultSets': [{
            'name': 'LeagueGameLog',
            'headers': FIELDS,
            'rowSet': rows,
        }]
    }


class ClientTests(unittest.TestCase):
    def test_cache_and_fresh_default(self):
        """By default a fresh fetch is made; reuse_cache avoids it."""
        calls = []

        def fetch(params, timeout):
            calls.append(params)
            return json.dumps(fixture())

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(d, min_interval=0, fetcher=fetch)

            # First call fetches from the network.
            self.assertEqual(len(client.game_logs('2022-23')), 2)

            # reuse_cache=True should use the stored snapshot.
            client.game_logs('2022-23', reuse_cache=True)
            self.assertEqual(len(calls), 1)

            # Without reuse_cache, a new fetch happens.
            client.game_logs('2022-23')
            self.assertEqual(len(calls), 2)

    def test_bad_pair(self):
        """A game missing one of its two team rows is rejected."""
        f = fixture()
        f['resultSets'][0]['rowSet'].pop()

        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(DataError):
                NBAClient(
                    d, attempts=1, fetcher=lambda p, t: json.dumps(f)
                ).game_logs('2022-23')

    def test_empty(self):
        """An empty response can't establish completeness and is rejected."""
        f = fixture()
        f['resultSets'][0]['rowSet'] = []

        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(DataError):
                NBAClient(
                    d, attempts=1, fetcher=lambda p, t: json.dumps(f)
                ).game_logs('2022-23')

    def test_conflict_saved_once(self):
        """A conflicting game is quarantined, the snapshot is saved once, and
        a venue override resolves the ambiguity on a later cached read."""
        f = fixture()
        # Make both rows claim to be the away team, so the pair is ambiguous.
        f['resultSets'][0]['rowSet'][0][FIELDS.index('MATCHUP')] = 'A @ B'

        calls = []

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d,
                min_interval=0,
                fetcher=lambda p, t: (calls.append(p) or json.dumps(f)),
            )

            # First call fails because of the unresolved home/away ambiguity.
            with self.assertRaises(DataError):
                client.game_logs('2022-23')
            self.assertEqual(len(calls), 1)

            # The raw snapshot and its quality sidecar should both be written.
            self.assertTrue(client.snapshot_path.exists())
            self.assertTrue(
                Path(str(client.snapshot_path) + '.quality.json').exists()
            )
            self.assertEqual(len(client.issues), 1)

            # Provide an override telling the client which team is home.
            client.venue_overrides = {
                '0022200001': {
                    'home_team_id': 2,
                    'away_team_id': 1,
                    'neutral_site': True,
                    'source': 'test schedule',
                }
            }

            # Reading from cache with the override now succeeds, without
            # a second network call.
            rows = client.game_logs('2022-23', reuse_cache=True)
            self.assertEqual(len(calls), 1)
            self.assertEqual([r['IS_HOME'] for r in rows], [False, True])
            self.assertTrue(all(r['NEUTRAL_SITE'] for r in rows))

    def test_partial(self):
        """With allow_partial=True, bad games are quarantined but good ones
        are still returned."""
        f = fixture()
        row_set = f['resultSets'][0]['rowSet']

        # Duplicate the valid pair under a new game ID, then corrupt one row
        # so it's ambiguous. The good pair should still be returned.
        extra = [r.copy() for r in row_set]
        for r in extra:
            r[FIELDS.index('GAME_ID')] = '0022200002'
        extra[0][FIELDS.index('MATCHUP')] = 'A @ B'
        row_set.extend(extra)

        with tempfile.TemporaryDirectory() as d:
            client = NBAClient(
                d, allow_partial=True, fetcher=lambda p, t: json.dumps(f)
            )
            self.assertEqual(len(client.game_logs('2022-23')), 2)
            self.assertEqual(client.issues[0]['game_id'], '0022200002')

    def test_readable_cache_and_migration(self):
        """Legacy snapshots are readable directly and can be migrated to the
        readable layout without touching the network."""
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)

            # Build a legacy-layout snapshot under leaguegamelog/<hash>/.
            params = dict(
                season='2022-23',
                season_type_all_star='Regular Season',
                player_or_team_abbreviation='T',
                league_id='00',
                date_from_nullable='',
                date_to_nullable='',
            )
            old = root / 'leaguegamelog' / legacy_key(params) / 'old.json'
            old.parent.mkdir(parents=True)
            old.write_text(json.dumps(dict(
                params=params,
                fetched_at='2022-10-19T00:00:00+00:00',
                response=json.dumps(fixture()),
            )))

            # The client should read the legacy snapshot without any fetch.
            client = NBAClient(
                root,
                fetcher=lambda p, t: self.fail('network not expected'),
            )
            self.assertEqual(
                len(client.game_logs('2022-23', reuse_cache=True)), 2
            )

            # Dry run reports success but writes nothing.
            self.assertEqual(migrate(root, False), 0)

            # Applying twice is safe: the second run is a no-op.
            self.assertEqual(migrate(root, True), 0)
            self.assertEqual(migrate(root, True), 0)

            # Exactly one migrated snapshot should exist in the readable layout.
            paths = list(
                (root / 'leaguegamelog' / '2022-23' /
                 'regular_season' / 'teams').glob('full_season_*.json')
            )
            self.assertEqual(
                len([p for p in paths if not p.name.endswith('.quality.json')]),
                1,
            )

            # Originals are retained.
            self.assertTrue(old.exists())

            # The migrated snapshot stores the response as a dict, not a string,
            # and round-trips through read_snapshot.
            new = next(
                p for p in paths if not p.name.endswith('.quality.json')
            )
            self.assertIsInstance(
                json.loads(new.read_text())['response'], dict
            )
            self.assertEqual(read_snapshot(new)[2], fixture())

            # After deleting the legacy file, the readable one is still used.
            old.unlink()
            self.assertEqual(
                len(client.game_logs('2022-23', reuse_cache=True)), 2
            )

            # A date-filtered fetch uses the "incremental" subfolder and a
            # "<start>_to_<end>_" filename prefix.
            client = NBAClient(
                root, fetcher=lambda p, t: json.dumps(fixture())
            )
            client.game_logs(
                '2022-23',
                start=date(2022, 10, 18),
                end=date(2022, 10, 18),
            )
            self.assertEqual(client.snapshot_path.parent.name, 'incremental')
            self.assertTrue(
                client.snapshot_path.name.startswith('20221018_to_20221018_')
            )
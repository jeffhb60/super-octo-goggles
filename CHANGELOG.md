# Changelog

All notable changes to the NBA forecasting project are documented here. Version numbers follow the project's GitHub release history.

## [0.4.0] — DuckDB analytical database

### Added
- Offline manifest-driven database builder (`ingestion/loaders/duckdb.py`) with dry-run validation and explicit replacement.
- Typed games, teams, players, team and player game statistics, snapshot provenance and build metadata tables.
- `game_results` view exposing signed home margin and home win labels for completed games only.
- Revalidation of snapshot hashes, game coverage and team/player relationships; rejection of incomplete manifests.
- Staged database rebuild so a failed build does not overwrite the existing database.
- Four loader tests (three passed offline; one DuckDB integration test requires DuckDB and pandas).

### Known limitations
- DuckDB integration and full historical performance are not verified in the development environment; install dependencies and run tests locally.
- Rebuilds use one explicitly selected completed manifest; incremental upserts and independent schedule completeness verification are future work.
- Unknown arena metadata stays null; upcoming games with unassigned teams are omitted. Team/player names are observed labels, not historical dimension versions.

## [0.3.0] — Historical bulk collection

### Added
- Configurable season-range collection for team and player regular-season and playoff game logs.
- Resumable collection with historical snapshot reuse, active-season refresh, and task-level failure isolation.
- Reconciliation of final game IDs and team coverage against season schedules.
- Atomic JSON collection manifests recording task outcomes and snapshot paths.
- Five offline tests for the historical collector, bringing the suite to 15 tests.

### Known limitations
- Completeness is verified relative to the available schedule, not an independent inventory of every historical game.
- Individual player completeness and older schedule reliability have not yet been established.
- Database loading is not included; live bulk collection verification is pending.

## [0.2.0] — Season schedule and venue resolution

### Added
- Injectable `schedule_fetcher` in `ingestion/clients/nba.py`, defaulting to a low-level `ScheduleLeagueV2` request through `NBAStatsHTTP` rather than the endpoint wrapper's dataframe parser.
- `season_schedule()` to retrieve or reuse schedules, preserve immutable snapshots under `scheduleleaguev2/<season>/`, and expose `schedule_snapshot_path` and `schedule`.
- `resolve_schedule` option in `game_logs()` to fetch schedule metadata before validation.
- Arena and tipoff annotations: `ARENA`, `ARENA_CITY`, `ARENA_STATE`, `ARENA_COUNTRY`, and `SCHEDULED_TIPOFF_UTC`.
- `ingestion/clients/schedule.py` with strict season and league checks, malformed and duplicate game-ID rejection, boolean-only `isNeutral` handling, and UTC timestamp normalization. Naive or implausible timestamps are discarded.
- `--resolve-schedule` and `--schedule-only` flags in `probe_nba.py`, including schedule coverage reporting, neutral-site summaries, and diagnostics for the previously disputed game.
- Schedule tests in `tests/test_schedule.py` covering ambiguous matchup resolution, arena annotations, schedule caching, conflicting team IDs, season mismatch, and absent neutral-site indicators.

### Changed
- Resolve home/away and venue metadata in this order: explicit `venue_overrides`, season schedule, then `MATCHUP`.
- Cross-check schedule team IDs against the game-log pair.
- Separate `_validate_team_pair()` from `_validate()` to isolate game-level checks from row-level validation and document the quarantine-versus-raise policy.
- Rename `tests/test_nba_client.py` to `tests/test_nba.py` to share the existing `fixture` helper with schedule tests, without changing existing test behavior.

## [0.1.0] — NBA ingestion foundation and readable cache

### Added
- NBA Stats ingestion around a human-readable, self-verifying on-disk snapshot format.
- Descriptive snapshot paths in `ingestion/clients/cache.py`: `leaguegamelog/<season>/<season_type>/<entity>/[incremental/]<range>_<ts>.json`.
- Embedded SHA-256 request and response hashes for integrity verification on read.
- Backward-compatible legacy cache reads and `candidates()` lookup across both layouts, newest first.
- `atomic_json()` and `save_snapshot()` helpers; snapshots are never overwritten, with a short UUID appended on filename conflicts.
- `venue_overrides` for ambiguous home/away designations, including neutral-site games.
- `allow_partial` support: strict mode raises on quarantined games; partial mode returns valid rows and records issues in `client.issues`.
- Quality reports (`<snapshot>.quality.json`) and the public `snapshot_path` attribute.
- Per-row and team-pair validation, including `PLUS_MINUS` consistency and `IS_HOME`, `NEUTRAL_SITE`, and `VENUE_SOURCE` annotations.
- `migrate_cache.py` for offline, non-destructive cache migration: dry run by default, `--apply` to copy snapshots and matching quality sidecars, nonzero exit on migration failures.
- `probe_nba.py` for API retrieval, summary reporting, full 2022–23 regular-season coverage checks, and exit code 2 when games are quarantined.
- Tests for fresh and cached requests, malformed pairs, empty responses, quality reports, venue overrides, partial results, legacy reads, and idempotent migration.
- Dependency constraint: `nba_api>=1.6,<3`.

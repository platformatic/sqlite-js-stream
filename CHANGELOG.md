# Changelog

All notable changes to `@platformatic/sqlite-js-stream` are documented here. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The first `sync()` after a restore into the replica's own path (as `ensureExists()` does) uploads
  only the changes made since the restore, instead of a snapshot of the whole database. The restore
  writes the first WAL of the restored database: one commit frame holding page 1 unchanged, under
  random salts recorded in `<db>-litestream/restore`. While the WAL keeps those salts, it holds every
  change made since the restore; any reset of the WAL changes them, and the next sync falls back to a
  snapshot. Databases not in WAL mode get no WAL and still start from a snapshot.

### Fixed

- `sync()` detects a WAL restarted since the previous sync from the salts in the WAL header. With
  `RESTART` checkpoints, frames of the old WAL left past the new ones could make it resume at a
  stale position and skip the new frames until the new WAL grew past it.
- A level-0 snapshot written by `sync()` records the WAL up to its last commit frame instead of the
  whole `-wal` file, so the next sync does not skip the frames of a transaction that was being
  written while the snapshot was taken.
- Writes made after restoring a database into the replica's own path (for
  example with `ensureExists()` on a new host) are replicated. The local position
  restarted at TXID 1, so `sync()` numbered the new changes with TXIDs the
  replica already had and never uploaded them. Restore now discards the local
  LTX cache and records the restored TXID, and the next `sync()` continues from
  it with a snapshot.
- Restore removes the `-wal` and `-shm` files left at the output path, which
  SQLite would otherwise replay on top of the restored database.

### Added

- `@platformatic/sqlite-js-stream-paged-vfs`, a new package: a SQLite VFS for
  `node:sqlite` that restores a database page by page on demand. Queries run
  while the database downloads; a read of a missing page waits until the
  application fills it. Experimental. Prebuilt for macOS arm64/x64 and Linux
  x64/arm64 (glibc 2.17+ or musl), one optional dependency per platform.
- `FileReplicaClient.deletePrefix()`, `PgReplicaClient.deletePrefix()` and
  `S3ReplicaClient.deletePrefix()` delete every replica under a prefix (the
  replica at the prefix and those under `prefix/`), generation and lease records
  included, and resolve with the prefixes of the deleted replicas. An optional
  `generation` fences the deletion (atomically with PostgreSQL, best effort with
  S3). The PostgreSQL deletion runs in one transaction, or in a savepoint of a
  transaction passed as `sql`. New exported types: `FileDeletePrefixOptions`,
  `PgDeletePrefixOptions`, `S3DeletePrefixOptions`.
- `Replica.checkpoint(conn, mode = 'TRUNCATE')` syncs, checkpoints the WAL through the
  application's writing connection (`RESTART` or `TRUNCATE`), and records that the next WAL
  continues from the synced position, so the next `sync()` writes an incremental LTX file instead
  of a full snapshot. Any other WAL reset still falls back to a snapshot. When replication runs on
  another thread than the writing connection, `Replica.prepareCheckpoint()` syncs and returns a
  JSON-serializable `CheckpointToken`, and the exported `runCheckpoint(conn, token, mode)` runs the
  checkpoint on the writing thread. New exported types: `CheckpointConnection`, `CheckpointMode`,
  `CheckpointToken`.
- `ErrReplicaAhead` (`ERR_REPLICA_AHEAD`), thrown by `sync()` when the replica
  has a higher TXID than the local database, instead of silently not uploading
  the local changes.
- Fenced writes for S3 (#18): `S3ReplicaClient` implements
  `registerGeneration()` and takes a `generation` option. Registering raises the
  generation in a `generation.json` object with conditional writes and records a
  fence at the replica's latest TXID; fenced uploads use `If-None-Match: *` and
  carry an `ltx-generation` metadata, and listings ignore files of a superseded
  writer above the fence. See "Fenced writes" in the README and
  docs/ARCHITECTURE.md for the protocol and its limits. New exports:
  `GenerationObjectName`, `GenerationMetadataKey`, `isFenced`,
  `GenerationState`, `GenerationFence`, and `getGeneration()` /
  `getGenerationState()` on the client.
- Fenced deletions: with a generation set, `deleteLtxFiles()` and `deleteAll()`
  (so compaction and retention) fail with `ErrGenerationSuperseded` once a
  higher generation is registered. PostgreSQL checks it in the deleting
  transaction; S3 checks it right before each `DeleteObjects` request.
- `Replica.superseded`: once a fenced client reports `ErrGenerationSuperseded`,
  the replica stops writing (every later write operation fails with that
  error), and `run()` stops and rejects with it instead of retrying.
- A shared behavior test suite for fencing backends
  (`test/helpers/fencing-suite.ts`), run against S3 (in-memory fake) and
  PostgreSQL (when `PG_URL` is set). The in-memory S3 fake honors `If-Match` /
  `If-None-Match` on writes and reads like S3.

### Changed

- `S3ReplicaClient.deleteAll()` keeps `generation.json`.
- `S3ReplicaClient` reads `generation.json` once per listing (a 404 when fencing
  is not used).
- `PgReplicaClient.deleteLtxFiles()` deletes all the given files in one
  transaction.

### Fixed

- `PgReplicaClient.init()` and `PgLeaser` create their tables under a
  transaction-level advisory lock, so several processes starting at once on a
  fresh database no longer fail with a unique violation on `pg_type`
  (concurrent `CREATE TABLE IF NOT EXISTS`).

### Added

- New PostgreSQL backend addon, **`@platformatic/sqlite-js-stream-pg`**:
  - `PgReplicaClient` implementing `ReplicaClient`, storing LTX files as a
    `ltx_files` row plus 1 MiB `ltx_chunks` rows, scoped by a `prefix` column in
    a configurable schema. Files are written atomically (row + chunks in one
    transaction) so a reader never sees a partial file, and the primary key makes
    a TXID range writable only once.
  - `PgLeaser` implementing `Leaser` via a compare-and-swap `version` column on
    an `ltx_leases` row (`LeaseExistsError`, `ErrLeaseNotHeld`).
  - Fenced writes: `client.registerGeneration(n)` rejects uploads from older
    generations with the new `ErrGenerationSuperseded` (`ERR_GENERATION_SUPERSEDED`)
    error, so a paused former writer can never append after losing its lease.
  - Registration of the `postgres://` / `postgresql://` URL schemes.

### Changed

- `compressBlock` is now a byte-identical TypeScript port of Go's
  `pierrec/lz4/v4` Fast block compressor, so LTX files we write match Go's
  `superfly/ltx` encoder exactly (validated against the Go goldens in CI's
  `interop-go` job). `lz4js` is retained only for LZ4 block decompression.
- `ReplicaClient` gained an optional `registerGeneration()` capability; existing
  backends are unaffected (it is optional).

## [0.1.0] - 2025-01-01

### Added

- Initial release.
- LTX format core: CRC-64 (ISO) checksums, header/page-frame/page-index/trailer
  encoding and decoding, LZ4 block codec (`lz4js`), validated against the
  [superfly/ltx](https://github.com/superfly/ltx) spec.
- SQLite WAL reader that parses header + frames and builds a page map.
- `ReplicaClient` interface plus a local **file** backend.
- `Replica` end-to-end replication: `sync()` (incremental WAL → LTX),
  `snapshot()` (full DB → LTX), `restore()`, streaming for large databases,
  multi-level compaction with retention/GC, and continuous `follow()` restore.
- Distributed leasing (`Leaser`) so `Replica.run()` elects a single writer.
- S3 backend as a separate addon (`@platformatic/sqlite-js-stream-s3`).
- Typed errors: every library error extends `SqliteJsStreamError` with a stable
  Node-style `code`.
- Distributed directly as type-stripped TypeScript source (Node ≥ 24, no build
  step) with accompanying `.d.ts` declarations.

### Notes

- Requires **Node >= 24** (native type stripping + `node:sqlite`).
- This library is a disaster-recovery / backup tool for a **single** SQLite
  database; it does not merge or reconcile data across hosts.

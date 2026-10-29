# Changelog

All notable changes to `@platformatic/sqlite-js-stream` are documented here. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Writes made after restoring a database into the replica's own path (for
  example with `ensureExists()` on a new host) are replicated. The local position
  restarted at TXID 1, so `sync()` numbered the new changes with TXIDs the
  replica already had and never uploaded them. Restore now discards the local
  LTX cache and records the restored TXID, and the next `sync()` continues from
  it with a snapshot.
- Restore removes the `-wal` and `-shm` files left at the output path, which
  SQLite would otherwise replay on top of the restored database.

### Added

- `ErrReplicaAhead` (`ERR_REPLICA_AHEAD`), thrown by `sync()` when the replica
  has a higher TXID than the local database, instead of silently not uploading
  the local changes.

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

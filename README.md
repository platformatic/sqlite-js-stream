# sqlite-js-stream

A **Node.js port of [Litestream](https://github.com/benbjohnson/litestream)** — SQLite streaming
replication and disaster recovery — written in **TypeScript** and built on the built-in `node:sqlite`.

SQLite is the most widely deployed database on the planet but it only lives on a single machine.
If that machine dies, your database is gone. This library replicates a SQLite database's **WAL
(Write-Ahead Log)** file into immutable, chronological **LTX (Log Transaction)** files on a remote
replica (S3, a local directory, etc.), and can **restore** a database from those files.

---

## Requirements

- **Node.js `>= 24`** — required for the built-in `node:sqlite`.
- Runtime dependency: `lz4js` (pure-JS LZ4 **block** decoder), matching the LTX format.
  Compression is a byte-identical TypeScript port of Go's `pierrec/lz4/v4` Fast block compressor.

The **published** package ships compiled JavaScript plus TypeScript declarations
(`dist/*.js` + `dist/*.d.ts`), built at publish time via `pnpm build`.

---

## Installation

This repository is a **pnpm workspace**. The core library lives at the root and
upload strategies (replica backends) live under `packages/`:

```bash
pnpm install       # link the workspace
pnpm build         # transpile src -> dist (.js + .d.ts), used by addons/tests
```

The core package is published as **`@platformatic/sqlite-js-stream`** and the S3
and PostgreSQL addons as **`@platformatic/sqlite-js-stream-s3`** and
**`@platformatic/sqlite-js-stream-pg`**:

```bash
pnpm add @platformatic/sqlite-js-stream
pnpm add @platformatic/sqlite-js-stream-s3  # optional addon
pnpm add @platformatic/sqlite-js-stream-pg   # optional addon
```

Importing the addon registers the `s3` URL scheme with
`newReplicaClientFromURL('s3://bucket/prefix')`.

---

## Quick start

```ts
import { Replica, FileReplicaClient } from '@platformatic/sqlite-js-stream';

// 1. Point the replica at your database path and a replica destination.
//    The library reads the database + `-wal` directly and is agnostic to
//    whichever SQLite driver your application uses.
const client = new FileReplicaClient('/data/replica');
const replica = new Replica('/data/app.db', client);

// 2. Sync builds a local LTX file (into <db>-litestream/ltx/0/) and uploads it.
await replica.sync();
```

The database is written to by your **own** SQLite connection (using `node:sqlite`, `better-sqlite3`,
or anything else). After more writes, just `sync()` again:

```ts
// ... your app writes rows with whatever driver you use ...
await replica.sync();

const status = await replica.syncStatus();
console.log(status.inSync, status.localTXID, status.remoteTXID); // true 3n 3n
```

### Take a full snapshot

Normal `sync()` writes **incremental** level-0 files. For a complete, point-in-time backup, take a
**level-9 snapshot** (a full database copy as an LTX file with `MinTXID = 1`):

```ts
await replica.snapshot();
```

### Restore a database

Restore reconstructs a SQLite database by compacting a level-9 snapshot with the subsequent
level-0 files:

```ts
await replica.restore({ outputPath: '/data/restored.db' });
```

Restore to a specific transaction ID, or to a point-in-time (the latest backup created at/before
the given time). Pass exactly one of `txID` or `timestamp`:

```ts
await replica.restore({ outputPath: '/data/app-at-txid.db', txID: 42n });
await replica.restore({ outputPath: '/data/app-at-time.db', timestamp: new Date('2025-06-01T12:00:00Z') });
```

Restoring to `txID`/`timestamp` is at **backup boundaries** (the end of an LTX file), and a target
with no matching backup throws `NoMatchingBackupError`. A "latest" restore over a non-contiguous
file set throws `RestoreGapError` rather than silently returning a partial database.

### Continuously restore (follow)

`follow()` restores the latest backup, then keeps the database up to date as new backups are
replicated — maintaining a read-only replica (litestream's `restore -f`):

```ts
const ac = new AbortController();
await replica.follow({ outputPath: '/data/replica.db', intervalMs: 1000, signal: ac.signal });
// later: ac.abort() to stop
```

The applied TXID is recorded in a `-txid` sidecar (default `<outputPath>-txid`) so a later `follow()`
resumes instead of re-restoring. The output database should only be opened **read-only**.

### Restore the local database if it is missing

```ts
await replica.ensureExists();
```

Restoring a replica's own database (`outputPath` equal to its path, as
`ensureExists()` does) makes it the writer from the restored position: the local
LTX cache of any previous database at that path is discarded, and the next
`sync()` uploads the following TXID, starting from a snapshot. Any `-wal` and
`-shm` files left at the output path are removed before the restored database
is moved into place. `sync()` throws `ErrReplicaAhead` (`ERR_REPLICA_AHEAD`)
rather than skipping uploads when the replica already has a higher TXID than the
local database.

### Manage many databases

```ts
import { Store } from '@platformatic/sqlite-js-stream';

const store = new Store();
await store.openDB('/data/a.db', new FileReplicaClient('/data/replica-a'));
await store.openDB('/data/b.db', new FileReplicaClient('/data/replica-b'));

await store.syncAll();      // local sync + upload for every database
await store.snapshotAll();  // level-9 snapshots for every database

store.close();
```

### Replica URL

Replica clients are created from URLs via the factory registry:

```ts
import { newReplicaClientFromURL, Replica } from '@platformatic/sqlite-js-stream';

const client = newReplicaClientFromURL('file:///data/replica');
const replica = new Replica('/data/app.db', client);
```

The `file` scheme is built into the core. Additional schemes (S3, etc.) are
registered by importing the matching addon (see below).

### Custom read connection

During sync and snapshot, a replica pins the WAL with a short read-only
`node:sqlite` transaction. To use another driver for that connection, e.g. for
libsql-encrypted databases that `node:sqlite` cannot open, pass `openReader`:

```js
import Database from 'libsql';

const replica = new Replica('/data/app.db', client, undefined, {
  openReader: (path) => new Database(path, { readonly: true, encryptionKey: process.env.DB_KEY }),
});
```

Pages are copied as-is, so the replica stays encrypted and a restored database
opens with the same key.

### S3 upload strategy

The `@platformatic/sqlite-js-stream-s3` addon provides an S3 replica client. Import it
once to register the `s3://` scheme, then create clients from URLs or directly:

```ts
import { Replica, newReplicaClientFromURL } from '@platformatic/sqlite-js-stream';
import { S3ReplicaClient } from '@platformatic/sqlite-js-stream-s3';

// Via a URL (bucket from the host, prefix from the path).
const client = newReplicaClientFromURL('s3://my-bucket/backup?region=us-east-1');

// Or directly, with explicit AWS config.
const s3 = new S3ReplicaClient({
  bucket: 'my-bucket',
  prefix: 'backup',
  region: 'us-east-1',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const replica = new Replica('/data/app.db', client);
await replica.run({ intervalMs: 1000 });
```

Objects are stored as `{prefix}/ltx/{level}/{minTXID}-{maxTXID}.ltx` — the same
layout as the file backend, so a replica can move between strategies. Creation
timestamps are preserved as the `ltx-timestamp` object metadata.

> **Testing without AWS:** `S3ReplicaClient` accepts an injected `client`
> implementing `send(command)`. Pass an in-memory fake (see
> `packages/backend-s3/test/helpers/memory-s3.ts`) to exercise the addon without
> network access.

### PostgreSQL upload strategy

The `@platformatic/sqlite-js-stream-pg` addon provides a PostgreSQL replica client.
Import it once to register the `postgres://` and `postgresql://` schemes, then
create clients from URLs or directly:

```ts
import { Replica, newReplicaClientFromURL } from '@platformatic/sqlite-js-stream';
import { PgReplicaClient, PgLeaser } from '@platformatic/sqlite-js-stream-pg';

// Via a URL: connection details from the URL, replica prefix from `?prefix=`.
const client = newReplicaClientFromURL(
  'postgres://user:pass@host:5432/mydb?prefix=app/db1',
);

// Or directly, with an explicit connection string.
const pg = new PgReplicaClient({
  url: 'postgres://user:pass@host:5432/mydb',
  prefix: 'app/db1',
});

const replica = new Replica('/data/app.db', client);
await replica.run({ intervalMs: 1000 });
```

Files are stored as one row in `ltx_files` plus one row per 1 MiB chunk in
`ltx_chunks`, scoped by a `prefix` column, in a configurable schema (`?schema=`).
Every file is written atomically (row + chunks in a single transaction), so a
reader never sees a partially-written file. `created_at` preserves the LTX
creation timestamp for point-in-time restore and retention.

> **Fenced writes:** see [Fenced writes](#fenced-writes).

> **Distributed leasing:** `PgLeaser` acquires/renews/releases a lease on a
> `ltx_leases` row using a compare-and-swap `version` column, with the same
> semantics as `S3Leaser` (`LeaseExistsError`, `ErrLeaseNotHeld`). Pass it to a
> `Replica` via the `leaser` option so only one host writes at a time.

### Fenced writes

When the application elects the writer of a database itself (with its own lease,
or a `Leaser`), a former writer that was paused (GC, a stopped VM, a network
partition) can wake up after losing the lease and keep uploading. The S3 and
PostgreSQL clients can reject those uploads: give every writer a *generation*
that only grows (for example a fencing token from your lease), and follow this
order when a writer takes over a database:

```ts
import { Replica } from '@platformatic/sqlite-js-stream';
import { S3ReplicaClient } from '@platformatic/sqlite-js-stream-s3';

const client = new S3ReplicaClient({ bucket: 'my-bucket', prefix: 'app/db1' });

// 1. Register: fails with ErrGenerationSuperseded if a higher generation is
//    registered. From now on, older writers' uploads and deletions fail.
await client.registerGeneration(token);

// 2. Restore (or keep the local database if it is at the replica's latest TXID).
const replica = new Replica('/data/app.db', client);
await replica.restore({ outputPath: '/data/app.db' });

// 3. Write and replicate.
await replica.sync();
```

`registerGeneration(n)` sets the client's generation (the `generation` option
does the same without registering). From then on:

- An upload, compaction or retention deletion fails with
  `ErrGenerationSuperseded` (`ERR_GENERATION_SUPERSEDED`) once a higher
  generation is registered. The `Replica` then stops writing: every later
  `sync()`, `snapshot()`, compaction and retention call fails with the same
  error (also in `replica.superseded`), and `run()` stops and rejects with it
  instead of retrying.
- An LTX file of a given level and TXID range is written only once.

**PostgreSQL** checks the generation in the transaction that writes or deletes
the files (`SELECT … FOR SHARE` on the `ltx_generations` row), so registering
waits for the uploads in flight and the following ones fail.

**S3** can't check one object and write another in one request, so fencing is a
protocol of conditional writes on a `generation.json` object next to the `ltx/`
directory:

- `registerGeneration(n)` raises the generation with `If-Match`/`If-None-Match`,
  then reads the replica's latest TXID `T` and records the *fence* `(n, T)` in
  the same object. It does both, so there is no separate step after the restore.
- Fenced uploads use `If-None-Match: *` and store their generation in the
  `ltx-generation` metadata. After each upload, the client reads
  `generation.json` and fails if a higher generation is registered. An upload
  that resolves was therefore made before any newer writer registered, and
  every newer writer's restore sees it.
- An upload of a superseded writer can still land if it was in flight. Listings
  ignore a file whose generation is lower than a fence's and whose `maxTXID` is
  above that fence's `T`. If such a file holds a key that the new writer needs,
  the new writer replaces it (`If-Match` on its ETag).
- Deletions check `generation.json` right before `DeleteObjects`. A deletion
  sent just before a new generation registers still applies; this is the one
  race the protocol leaves open, and it needs the old writer to pause between
  the check and the request.
- Once a fence exists, listing a file that ends above the lowest fence costs
  one `HeadObject` request to read its generation (cached per key and ETag; a
  writer knows its own files). Replicas that never registered a generation pay
  one `GetObject` of `generation.json` per listing (a 404).
- `deleteAll()` keeps `generation.json`, so a superseded writer stays fenced.

The file backend does not implement fenced writes.

### Deleting every replica under a prefix

`deleteAll()` deletes the files of one replica and keeps its generation, so a
superseded writer stays fenced. To delete a whole group of replicas, for
example every database of a tenant, each client has a static `deletePrefix()`.
It deletes the replica at the prefix and every replica under `prefix/` (`a`
covers `a` and `a/b`, not `ab`), with their generations and leases, and resolves
with the sorted prefixes (paths for the file backend) of the deleted replicas:

```ts
import { FileReplicaClient } from '@platformatic/sqlite-js-stream';
import { PgReplicaClient } from '@platformatic/sqlite-js-stream-pg';
import { S3ReplicaClient } from '@platformatic/sqlite-js-stream-s3';

await FileReplicaClient.deletePrefix({ path: '/backups/tenant-1' });
await PgReplicaClient.deletePrefix({ sql, prefix: 'tenant-1', tablePrefix: 'ltx' });
await S3ReplicaClient.deletePrefix({ bucket: 'my-bucket', prefix: 'tenant-1', client });
// => ['tenant-1/db1', 'tenant-1/db2']
```

The options take the same connection settings as the client's constructor (`url`
or `sql`, `schema` and `tablePrefix` for PostgreSQL; `client` or `region`,
`endpoint` and credentials for S3). An empty prefix is rejected.

With a `generation` option, the deletion is fenced like the deletions of a fenced
client: it fails with `ErrGenerationSuperseded` if a replica under the prefix has
a higher generation.

- **PostgreSQL** deletes everything in one transaction that first locks the
  generation rows (`FOR UPDATE`): it waits for the fenced uploads and deletions
  in flight, the ones that start meanwhile wait for it, and a superseded
  deletion deletes nothing. Pass a transaction as `sql` (the argument of a
  `sql.begin()` callback) to run the deletion in a savepoint of it, so it commits
  or rolls back with the caller's own changes.
- **S3** is best effort: the generation of each replica is checked before
  deleting its first object and again before the generation objects, which are
  deleted last, so former writers stay fenced while the files are deleted. A
  generation registered between a check and a `DeleteObjects` request does not
  stop the deletion, and a superseded deletion keeps what it already deleted.
- **The file backend** has no generations: the directory is removed.

Once the generations are deleted, nothing fences the former writers of these
replicas: a writer still running can upload again and create its replica anew.
Stop the writers first.

### Continuous sync

For ongoing replication, `replica.run()` syncs immediately and then repeats on an
interval until the supplied `AbortSignal` is aborted. Sync errors are logged and the
loop keeps going, so a transient failure doesn't stop replication:

```ts
import { Replica, FileReplicaClient } from '@platformatic/sqlite-js-stream';

const replica = new Replica('/data/app.db', new FileReplicaClient('/data/replica'));
const ac = new AbortController();

// Run continuously (sync now, then every 1s); resolves when aborted.
const runP = replica.run({ intervalMs: 1000, signal: ac.signal });

// ... later, to stop:
ac.abort();
await runP;
```

If you want to keep replicating in the background without awaiting, call `run()` and
`catch` its promise (it resolves on abort):

```ts
replica.run({ intervalMs: 1000, signal: ac.signal }).catch((err) => {
  console.error('continuous sync stopped', err);
});
```

`Store` also exposes `syncAll()`/`snapshotAll()` for one-shot operations; a continuous
loop can be driven by calling `replica.run()` per database, or via your own scheduler.

### Checkpoints without full snapshots

When the WAL is reset by a checkpoint, the next `sync()` cannot resume from its previous position
in the WAL, so it uploads a full snapshot of the database. An application that decides when
checkpoints run can avoid that with `replica.checkpoint()`: it syncs, checkpoints the WAL, and
records that the next WAL continues from the synced position, so the next `sync()` uploads only the
changes.

```ts
import { DatabaseSync } from 'node:sqlite';

const db = new DatabaseSync('/data/app.db');
db.exec('PRAGMA journal_mode=WAL');
db.exec('PRAGMA wal_autocheckpoint=0'); // checkpoints run only through replica.checkpoint()

// ... write with db, and call replica.sync() as usual ...

// Instead of db.exec('PRAGMA wal_checkpoint(TRUNCATE)'):
const incremental = await replica.checkpoint(db, 'TRUNCATE');
```

`checkpoint(conn, mode = 'TRUNCATE')` takes the connection the application writes with (`mode` is
`'RESTART'` or `'TRUNCATE'`):

- Pass the **writing connection**, with no open transaction. SQLite gives the next WAL a predictable
  salt only when the connection that starts it is the one that reset the WAL; that is how the next
  `sync()` recognizes the WAL that directly followed the checkpoint.
- Disable `wal_autocheckpoint` on every connection and never run `PRAGMA wal_checkpoint` yourself:
  any other reset is detected and the next `sync()` falls back to a full snapshot (correct, but
  not incremental).
- Do not call it concurrently with `sync()` or `run()`.
- The next WAL must be started by the same connection: a different connection that writes first
  picks random salts, and the next `sync()` falls back to a snapshot.

When replication runs on a different thread (or process) than the writing connection, which
cannot cross threads, split the call in two. `replica.prepareCheckpoint()` syncs and returns a
plain JSON token; send it to the writing thread and pass it to `runCheckpoint()` with the
connection:

```ts
// Replication thread:
const token = await replica.prepareCheckpoint(); // { path, txid, salt1, salt2, frames }
port.postMessage({ type: 'checkpoint', token });

// Writing thread (owns `db`), between transactions:
import { runCheckpoint } from '@platformatic/sqlite-js-stream';
const incremental = runCheckpoint(db, token, 'TRUNCATE'); // synchronous
```

`runCheckpoint()` writes the record into `<db>-litestream/` next to the database, and the
replica's next `sync()` reads it from disk, so the two sides share nothing else. No `sync()` of
the database may run between `prepareCheckpoint()` and `runCheckpoint()`, and the writing thread
must not commit between receiving the token and calling `runCheckpoint()`; a commit makes the
checkpoint return `false`, which is safe but not incremental. `replica.checkpoint(conn, mode)` is
`prepareCheckpoint()` followed by `runCheckpoint()`.

It returns `true` when the WAL was fully checkpointed right after the sync. It returns `false` when
a commit landed between the sync and the checkpoint, or when a reader kept the checkpoint from
completing: the next `sync()` then resumes in the old WAL if it was not reset, or uploads a
snapshot if it was. Restoring across checkpoints works as usual: the incremental files form a
contiguous TXID chain.

### Replication from a worker thread

Run `replica.run()` in a **worker thread** so continuous replication doesn't compete with
your application's main thread. The worker opens its own **read-only** connection (via the
library) and reads the database + `-wal` files, while your app keeps writing from the main
thread on its own connection. Only the **path**s need to cross threads.

`replica-worker.ts`:

```ts
import { parentPort } from 'node:worker_threads';
import { Replica, FileReplicaClient } from '@platformatic/sqlite-js-stream';

parentPort.on('message', async (msg) => {
  if (msg?.type !== 'start') return;
  const { dbPath, clientPath, intervalMs = 1000 } = msg;

  const replica = new Replica(dbPath, new FileReplicaClient(clientPath));
  const ac = new AbortController();
  const runP = replica.run({ intervalMs, signal: ac.signal });

  parentPort.postMessage({ type: 'started' });
  parentPort.on('message', async (m) => {
    if (m?.type === 'stop') {
      ac.abort();
      await runP;
      parentPort.postMessage({ type: 'done' });
    }
  });
});
```

Main thread:

```ts
import { Worker } from 'node:worker_threads';

const worker = new Worker(new URL('./replica-worker.ts', import.meta.url));
const started = new Promise((resolve) => worker.on('message', (m) => m?.type === 'started' && resolve()));
const done = new Promise((resolve) => worker.on('message', (m) => m?.type === 'done' && resolve()));

worker.postMessage({ type: 'start', dbPath: '/data/app.db', clientPath: '/data/replica', intervalMs: 1000 });
await started;

// Your app keeps using node:sqlite (or better-sqlite3) from the main thread.
// ... write rows ...

// Shut the replication worker down gracefully.
worker.postMessage({ type: 'stop' });
await done;
await worker.terminate();
```

> **Important:** the worker and your app each use their **own** connection to the same database
> file. Never hand the worker the same `DatabaseSync` handle your app writes with — a single
> connection holds one transaction at a time, so sharing a writable handle would merge the read
> snapshot with your writes (see *Concurrency & snapshot consistency* above).

---

## How it works

Litestream (and this port) never touches the database through the SQLite WAL/VFS as part of
replication. Instead it:

1. **Reads the `-wal` file directly** (parsing frames, verifying the WAL salt + checksum) and the
   **database file directly** via `fs`.
2. **Builds an LTX file** per sync — a sequence of LZ4-compressed, checksummed pages that capture
   the changed pages, tagged with a sequential **transaction ID**.
3. **Uploads** those immutable LTX files to a replica.
4. **Restores** by compacting a snapshot with the stream of level-0 files and writing a valid
   SQLite file.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design and
[`docs/LTX_FORMAT.md`](docs/LTX_FORMAT.md) for the on-disk format.

### Concurrency & snapshot consistency

To keep snapshots consistent, `sync()`/`snapshot()` open a **separate, read-only** SQLite
connection to the same database and hold a short-lived read transaction while they read the
`-wal` + database files. SQLite will not checkpoint/truncate the WAL past an active reader, and the
pinned reader's `PRAGMA page_count` reports the commit including uncheckpointed WAL pages.

Your application is free to use its own SQLite driver and connections (`node:sqlite`,
`better-sqlite3`, …) independently — the library reads only the files and never touches them.

> **Do not point the library at your application's writable connection.**
>
> A single SQLite connection holds **one transaction at a time**. If the library were to use the
> same `DatabaseSync` handle as your writer:
> - its read snapshot would **not be isolated** from concurrent app writes (they'd merge into one
>   transaction, and the library could read *uncommitted* rows or commit the app's writes), and
> - if your app had a transaction open, the library could not `BEGIN` its snapshot pin at all.
>
> The library deliberately opens its own read-only reader instead. The only API that takes your
> writable connection is the checkpoint (`replica.checkpoint()` / `runCheckpoint()`), which runs
> `PRAGMA wal_checkpoint` on it and never reads through it. Keep the library's reader and your app's writer on separate
> connections to the same database file.

### Large databases & streaming

The replication path is **streaming** end-to-end, so peak memory is bounded by one page buffer
(plus the small LTX page index) rather than the database size:

- `sync()`/`snapshot()` read database **pages one at a time from a file handle** and stream the
  encoded LTX straight to a temp file (never buffering the whole DB/WAL).
- `restore()` streams each LTX file's pages into the output database, overlaying incremental files
  on the snapshot base — it never materializes a full merged file or the whole DB in memory.
- The S3 addon uploads objects larger than **5 MiB** with the **multipart** API, so a large snapshot
  is never buffered on the upload side either.

### Benchmarks

Reproducible, configurable benchmarks live in [`bench/bench-sync-restore.ts`](bench/bench-sync-restore.ts)
and measure `sync()`/`restore()` throughput plus peak process RSS:

```bash
pnpm bench
# another size / batch:
ROWS=200000 BATCH=1000 pnpm bench
# demonstrate the streaming memory bound (peak RSS vs DB size):
SCALE=4 pnpm bench
```

Conditions are disclosed in the output (rows, batch/transactions, source WAL bytes). Peak RSS is
process-wide (it includes the Node baseline), so the meaningful claim is that it stays **flat** as the
database grows — see the `SCALE` mode, which shows WAL bytes and peak RSS at two sizes.

### Compaction & retention

Over time `sync()` writes one level-0 LTX file per transaction. To keep the replica bounded and
restore fast, compact those into higher levels and prune data that has been superseded:

```ts
// Merge pending level-0 files into a level-1 file (and 1→2→3 up to maxCompactionLevel).
await replica.compact({ dstLevel: 1 });

// Time-based retention: delete snapshots older than 24h (always keeping the
// newest), cascade the boundary down through the other levels, and delete
// level-0 files that were compacted into level-1 once they age past 5m.
const result = await replica.enforceRetention({
  snapshotRetentionMs: 24 * 60 * 60 * 1000,
  l0RetentionMs: 5 * 60 * 1000,
});
```

`Replica.run()` can drive all of this on their own schedules, and `Store` orchestrates it across
every replica.

```ts
import { CompactionLevels } from '@platformatic/sqlite-js-stream';

// Each level is compacted at most once per wall-clock bucket (L1 every 30s,
// L2 every 5m, L3 every 1h) — mirrors litestream's per-level pacing.
const levels = new CompactionLevels([
  { level: 0, intervalMs: 0 },
  { level: 1, intervalMs: 30_000 },
  { level: 2, intervalMs: 300_000 },
  { level: 3, intervalMs: 3_600_000 },
]);

await store.run({
  intervalMs: 1000,
  compactionLevels: levels,
  snapshotIntervalMs: 24 * 60 * 60 * 1000,
  snapshotRetentionMs: 24 * 60 * 60 * 1000,
  l0RetentionMs: 5 * 60 * 1000,
  signal,
});
```

Compaction is **streaming** (bounded memory) and only deletes a lower-level file after the higher
level is written *and* uploaded, so a mid-compaction failure never loses data. Retention never deletes
files needed to restore any point within the retention window, and the live-head level-0 file is
always retained.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#compaction--retention) for the full model.

### Distributed leasing

When several replicas point at the same storage, only one may write at a time. Give the replica a
`Leaser` (a `lock.json` lease) and `run()` will make exactly one replica the active **primary**;
the others become idle **followers** that poll until the lease is free. Storage writes are gated on
lease ownership, so a secondary can never split the brain.

```ts
import { FileLeaser } from '@platformatic/sqlite-js-stream';

replica.leaser = new FileLeaser('/data/replica'); // becomes primary if the lease is free
await replica.run({ intervalMs: 1000, signal });

// Query which mode we are in:
if (replica.isPrimary()) {
  // we are writing
}
```

The file backend uses an exclusive-create lease with a content-hash token; the S3 addon
(`import { S3Leaser } from '@platformatic/sqlite-js-stream-s3'`) uses S3 `If-None-Match`/`If-Match`
conditional writes so racy acquirers cannot both win.

### ⚠️ This is a backup tool, not a distributed database

The lease coordinates **which host writes to storage**. It does **not** merge, compare, or reconcile
the actual data in your databases. Be very clear about what you're pointing at the same prefix.

**Multiple hosts are only safe if they hold the *same* database content** (e.g. a DR/standby host that
was restored from the same backup). The lease then prevents two hosts from both writing, and gives
you automatic failover when the primary stops.

**Do NOT point hosts that hold *different* data at the same prefix.** SQLite is a
single-writer, single-file database. If host A has `row id=1 → "X"` and host B has
`row id=1 → "Y"` and they share one prefix, here is what happens:

| Host A local DB      | Host B local DB      | What lands in S3 |
|----------------------|---------------------|------------------|
| `row id=1 → "X"`     | `row id=1 → "Y"`     | `"X"` (only if **A** is the primary) |

- Whichever host is the **primary** gets backed up. The other host's value is **never uploaded**, is
  not merged, and is effectively **lost** from the backup's perspective.
- When you restore, you get exactly one host's state. There is **no conflict resolution**. `"Y"` is
  simply gone.

**Choose the right setup:**

| If you need…                                            | Do this |
|---------------------------------------------------------|----------|
| Each host owns a **different** database                 | Give each its own prefix, e.g. `app/${tenantId}` — separate backup per DB, no conflict. |
| Multiple app instances sharing one consistent dataset | Don't use SQLite + this replication. Use a client-server DB (Postgres/MySQL) or a real shared-file lock + single writer. |
| Same data on every host (failover / DR)                 | Keep exactly one writer. On failover, **restore the standby from S3 first**, then let it take the lease — don't let a stale local file start writing. |

---

## Deferred features

Not implemented — listed here so the roadmap is clear. None of these are required for the library
core to replicate, snapshot, compact, lease, and restore a SQLite database.

- **CLI + YAML config** — this is a *library*. The `litestream replicate/restore/ltx/reset/snapshots`
  CLI and a `litestream.yml`-style config are out of scope for now.
- **Metrics/telemetry** — no Prometheus/OpenTelemetry instrumentation yet.
- **VFS real-time mirroring** — hooking into SQLite's VFS to mirror writes live is not implemented.
- **IPC control server** — litestream's Unix-socket IPC server is not implemented.

---

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system design (DB, Replica, ReplicaClient,
  LTX, WAL, backends).
- [`docs/LTX_FORMAT.md`](docs/LTX_FORMAT.md) — the LTX binary format.
- [`test/`](test) — TDD test suite (`npm test`).

---

## Credits

This project is a **port of [Litestream](https://github.com/benbjohnson/litestream)**
by [Ben Johnson](https://github.com/benbjohnson) (Apache-2.0). The LTX file format and much of the
design come from the [`superfly/ltx`](https://github.com/superfly/ltx) library and the litestream
documentation (`docs/ARCHITECTURE.md`, `docs/SQLITE_INTERNALS.md`, `docs/LTX_FORMAT.md`,
`docs/PATTERNS.md`), which are adapted here with attribution.

The SQLite **WAL** and **LTX** formats are replicated faithfully so that this library can read LTX
files produced by Litestream and vice-versa.

---

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

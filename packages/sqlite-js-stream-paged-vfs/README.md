# @platformatic/sqlite-js-stream-paged-vfs

A SQLite VFS for `node:sqlite` that reads the pages of a database on demand.

A registered database is a sparse file with the final size of the database, and
all its pages are missing at first. When SQLite reads a missing page, the VFS
asks your code for it and blocks the reading thread until you fill it. You fill
the other pages in the background. Once every page is present, the database is
a regular SQLite database and reads are local only.

It lets an application open a large database restored from a replica right away,
instead of after downloading all of it. To restore a database replicated with
[`@platformatic/sqlite-js-stream`](https://github.com/platformatic/sqlite-js-stream#restore-page-by-page),
call `replica.restorePaged()`: it uses this package to fetch each page from the
replica when it is read, and fills the others in the background.

> Status: experimental.

## Install

```sh
npm install @platformatic/sqlite-js-stream-paged-vfs
```

It requires Node.js 24 or later. The native library is prebuilt for:

| OS    | Architectures | C library          |
| ----- | ------------- | ------------------ |
| macOS | arm64, x64    |                    |
| Linux | x64, arm64    | glibc 2.17+        |
| Linux | x64, arm64    | musl (e.g. Alpine) |

Each build is a separate package, such as
`@platformatic/sqlite-js-stream-paged-vfs-linux-x64-gnu`, which npm installs as
an optional dependency on the matching platform. Installing with
`--omit=optional` leaves it out. Windows is not supported.

## Usage

```js
import { databaseUri, register } from '@platformatic/sqlite-js-stream-paged-vfs';

const db = register({ path: '/data/large.db', pageSize: 4096, pageCount: 250_000 }, (pgno) => {
  // A reader waits for this page: fetch it first, e.g. with a ranged read of a replica
  fetchPage(pgno).then((page) => db.fill(pgno, page), () => db.fail(pgno));
});

// Fill the other pages in the background
for (const pgno of db.missingPages()) {
  db.fill(pgno, await fetchPage(pgno));
}
```

Read the database from another thread, such as a worker, with the URI of the
VFS:

```js
const connection = new DatabaseSync(databaseUri('/data/large.db'));
```

## Example: query a database while it downloads

This example restores `source.db` to `restored.db`. `source.db` stands in for a
replica in object storage, with 20 ms of latency per read. Four worker threads
query the database right away, while the main thread fetches the pages they
wait for, and downloads the others in the background.

Save the three files in a directory where the package is installed, then run:

```sh
node create-sample.ts
node main.ts
```

`create-sample.ts` creates the sample database:

```ts
// Creates source.db, a sample database with 500,000 users (about 20 MB).

import { rmSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

rmSync('source.db', { force: true });

const db = new DatabaseSync('source.db');
db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);

  INSERT INTO users (name, email)
    WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500000)
    SELECT 'User ' || i, 'user' || i || '@example.com' FROM n;
`);
db.close();
```

`worker.ts` answers queries:

```ts
// Answers queries on the restored database. It runs in a worker thread, as a
// read of a missing page blocks the thread until the main thread fills it.

import { DatabaseSync } from 'node:sqlite';
import { parentPort, workerData } from 'node:worker_threads';

const db = new DatabaseSync(workerData.uri);
const findUser = db.prepare('SELECT id, name, email FROM users WHERE id = ?');

parentPort?.on('message', ({ id, userId }: { id: number; userId: number }) => {
  try {
    parentPort?.postMessage({ id, user: findUser.get(userId) });
  } catch (err) {
    parentPort?.postMessage({ id, error: (err as Error).message });
  }
});
```

`main.ts` registers the database, fills its pages, and sends queries to the
workers:

```ts
// Restores source.db to restored.db page by page, while worker threads query
// it. source.db stands in for a replica in object storage: each read of it
// waits 20 ms, like a ranged GET would.

import { open, rm } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as sleep } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

import { databaseUri, register } from '@platformatic/sqlite-js-stream-paged-vfs';

const SOURCE = 'source.db';
const TARGET = 'restored.db';

const replica = await open(SOURCE);

/** Reads a range of the replica. With object storage, this is a GET with a Range header. */
async function read(offset: number, length: number): Promise<Buffer> {
  await sleep(20);
  const buffer = Buffer.alloc(length);
  await replica.read(buffer, 0, length, offset);
  return buffer;
}

// The header of a database holds its page size and its page count
const header = await read(0, 100);
const pageSize = header.readUInt16BE(16) === 1 ? 65536 : header.readUInt16BE(16);
const pageCount = header.readUInt32BE(28);

// SQLite would apply the journals of a previous database to the new one
for (const suffix of ['', '-wal', '-shm', '-journal']) {
  await rm(TARGET + suffix, { force: true });
}

// The VFS calls this on the main thread when a worker waits for a missing page
let requested = 0;
const db = register({ path: TARGET, pageSize, pageCount }, pgno => {
  requested++;
  read((pgno - 1) * pageSize, pageSize).then(
    page => db.fill(pgno, page),
    () => db.fail(pgno), // Fails the read; the next read requests the page again
  );
});

// Meanwhile, download every page in order, 1 MiB at a time
async function download(): Promise<void> {
  const batch = (1024 * 1024) / pageSize;

  for (let first = 1; first <= pageCount; first += batch) {
    const count = Math.min(batch, pageCount - first + 1);
    const pages = await read((first - 1) * pageSize, count * pageSize);

    for (let i = 0; i < count; i++) {
      // Does nothing if the page is already present
      db.fill(first + i, pages.subarray(i * pageSize, (i + 1) * pageSize));
    }
  }
}

const start = performance.now();
const downloaded = download().then(() => performance.now() - start);

// The workers open the database through the VFS, and query it right away
const workers = Array.from({ length: 4 }, () => new Worker(new URL('./worker.ts', import.meta.url), {
  workerData: { uri: databaseUri(TARGET) },
}));

const pending = new Map<number, PromiseWithResolvers<unknown>>();
let nextId = 0;

for (const worker of workers) {
  worker.on('message', ({ id, user, error }: { id: number; user?: unknown; error?: string }) => {
    const request = pending.get(id)!;
    pending.delete(id);

    if (error) {
      request.reject(new Error(error));
    } else {
      request.resolve(user);
    }
  });
}

function findUser(userId: number): Promise<unknown> {
  const id = nextId++;
  const request = Promise.withResolvers<unknown>();
  pending.set(id, request);
  workers[id % workers.length].postMessage({ id, userId });
  return request.promise;
}

const users = await Promise.all([1, 123_456, 250_000, 499_999].map(findUser));
const answered = performance.now() - start;

console.log(users);
console.log(`Answered in ${answered.toFixed(0)} ms, with ${requested} pages requested on demand`);
console.log(`Downloaded all ${pageCount} pages in ${(await downloaded).toFixed(0)} ms`);

await Promise.all(workers.map(worker => worker.terminate()));
await replica.close();

// Once complete, the database is unregistered: it is a regular database, which
// any thread can open, with or without the VFS
console.log(`Complete: ${db.complete}`);

const local = new DatabaseSync(TARGET);
const { integrity_check: integrity } = local.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
console.log(`Integrity check: ${integrity}`);
local.close();
```

The workers get their answers after a handful of pages, long before the whole
database is downloaded:

```text
[
  { id: 1, name: 'User 1', email: 'user1@example.com' },
  { id: 123456, name: 'User 123456', email: 'user123456@example.com' },
  { id: 250000, name: 'User 250000', email: 'user250000@example.com' },
  { id: 499999, name: 'User 499999', email: 'user499999@example.com' }
]
Answered in 80 ms, with 7 pages requested on demand
Downloaded all 5226 pages in 487 ms
Complete: true
Integrity check: ok
```

The gap grows with the size of the database: the queries need about the same
number of pages, while the download takes longer.

## Rules

- The thread which registered a database receives its requests, so it must not
  read the database while it is incomplete: it would wait for itself. Such reads
  fail at once.
- Every connection that writes to a registered database must open it with
  `databaseUri()`. A connection that does not use the VFS reads missing pages as
  zeros.
- Register a database before any connection opens it. A connection opened
  earlier does not see the registration.
- Remove any `-wal` file left by a previous database at the same path before
  registering: SQLite would apply it to the new one.
- `load()` fails when the Node.js permission model is enabled, as it forbids
  SQLite extensions.

## API

### `register(options, onRequest)`

Creates the file (or truncates it) to `pageCount` pages, all missing, and
returns a `PagedDatabase`. Options:

- `path`: the database file.
- `pageSize`: a power of two between 512 and 65536.
- `pageCount`: the number of pages of the database.
- `timeout`: how long a read waits for a page, in milliseconds. Defaults to
  30000.

`onRequest(pgno)` is called once per missing page that a reader waits for. A page
is requested again after a failure or a timeout.

### `PagedDatabase`

- `fill(pgno, page)`: writes a page unless it is present. Returns whether it was
  written.
- `fail(pgno)`: fails the reads waiting for a page.
- `missingPages()`: the numbers of the missing pages, in ascending order.
- `missing`, `complete`, `pageSize`, `pageCount`.
- `close()`: unregisters the database. Reads of missing pages fail from then on.

A database is unregistered automatically once complete.

### `databaseUri(path)`

Returns the SQLite URI which opens `path` with the VFS.

### `VFS_NAME`

The name of the VFS, `sqlite-js-stream-paged-vfs`, to build URIs yourself.

### `load()`

Loads the library and registers the VFS. `register()` calls it.

### `libraryPath()`

Returns the path of the native library for this platform.

## How it works

The package is a native library written in Rust with [napi-rs](https://napi.rs).
The same file is:

- a SQLite loadable extension, which registers the VFS in the SQLite embedded in
  Node.js. Node.js does not export the functions of its SQLite, so
  `DatabaseSync#loadExtension()` is the only way to reach it.
- a Node.js addon, which registers the databases and fills their pages.

`load()` loads the file both ways. The VFS is registered once for the whole
process, so every thread can then open a database with it.

The VFS wraps the default VFS of SQLite:

- Reading a missing page calls the `onRequest` callback of the database, on the
  thread which registered it, then waits until the page is filled, failed, or
  the timeout expires. A failed or late page fails the read with
  `SQLITE_IOERR_READ`.
- Writing a page marks it present. Filling a page that is already present does
  nothing, so a fill never overwrites a newer version written by SQLite.
- The VFS never maps the database in memory, so every read goes through it.

## Development

Building needs a Rust toolchain.

```sh
pnpm build:native   # builds sqlite-js-stream-paged-vfs.<platform>.node
pnpm test
```

To build for another platform, pass `--target`. Linux glibc builds use
`--use-napi-cross`, and musl builds use `--cross-compile`, which needs
[zig](https://ziglang.org). For example:

```sh
pnpm build:native --target aarch64-unknown-linux-musl --cross-compile
```

The `paged-vfs-binaries` workflow builds the library for every platform, and
tests each build on its platform. The release workflow runs it, publishes one
package per platform, then this package with the platform packages as optional
dependencies.

## License

Apache-2.0

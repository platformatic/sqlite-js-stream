# @platformatic/sqlite-js-stream-paged-vfs

A SQLite VFS for `node:sqlite` that reads the pages of a database on demand.

A registered database is a sparse file with the final size of the database, and
all its pages are missing at first. When SQLite reads a missing page, the VFS
asks your code for it and blocks the reading thread until you fill it. You fill
the other pages in the background. Once every page is present, the database is
a regular SQLite database and reads are local only.

It lets an application open a large database restored from a replica right away,
instead of after downloading all of it.

> Status: experimental, and not published yet. It supports Linux and macOS.

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

## Usage

```js
import { DatabaseSync } from 'node:sqlite';
import { databaseUri, register } from '@platformatic/sqlite-js-stream-paged-vfs';

const db = register({ path: '/data/large.db', pageSize: 4096, pageCount: 250_000 }, async (pgno) => {
  try {
    db.fill(pgno, await fetchPage(pgno)); // your code, e.g. a ranged read of a replica
  } catch {
    db.fail(pgno);
  }
});

// Fill the other pages in the background
for (const pgno of db.missingPages()) {
  db.fill(pgno, await fetchPage(pgno));
}

console.log(db.complete); // true
```

Read the database from another thread, for example a worker:

```js
const connection = new DatabaseSync(databaseUri('/data/large.db'));
```

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

### `load()`

Loads the library and registers the VFS. `register()` calls it.

## Development

Building needs a Rust toolchain.

```sh
pnpm build:native   # builds paged-vfs.<platform>.node
pnpm test
```

## License

Apache-2.0

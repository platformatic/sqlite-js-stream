import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import {
  VFS_NAME,
  databaseUri,
  libraryPath,
  load,
  register,
  type PagedDatabase,
  type RegisterOptions,
} from '../src/index.ts';
import { createSource, read, tempDir, type Source } from './helpers/source.ts';

/** Registers a database whose missing pages are filled from `source`, after `delay` milliseconds. */
function serve(
  t: TestContext,
  path: string,
  source: Source,
  options: Partial<RegisterOptions> = {},
  delay = 0,
): { db: PagedDatabase; requests: number[] } {
  const requests: number[] = [];
  const db = register({ path, pageSize: source.pageSize, pageCount: source.pageCount, ...options }, pgno => {
    requests.push(pgno);
    setTimeout(() => db.fill(pgno, source.page(pgno)), delay);
  });

  t.after(() => db.close());
  return { db, requests };
}

function fillAll(db: PagedDatabase, source: Source): number {
  let written = 0;

  for (let pgno = 1; pgno <= source.pageCount; pgno++) {
    if (db.fill(pgno, source.page(pgno))) {
      written++;
    }
  }

  return written;
}

test('load() registers the VFS in node:sqlite', (t) => {
  load();
  load();

  assert.throws(() => new DatabaseSync('file:missing-vfs.db?vfs=missing-vfs'), /no such vfs/);

  // A database which is not registered is a regular one
  const db = new DatabaseSync(databaseUri(join(tempDir(t), 'plain.db')));
  db.exec('PRAGMA journal_mode = WAL; CREATE TABLE t (v); INSERT INTO t VALUES (1)');
  assert.deepEqual({ ...db.prepare('SELECT v FROM t').get() }, { v: 1 });
  assert.ok(databaseUri('/a b/c.db').endsWith(`?vfs=${VFS_NAME}`));
  db.close();
});

test('reads only the pages SQLite uses, on demand', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const { db, requests } = serve(t, path, source);

  assert.equal(db.missing, source.pageCount);
  assert.equal(db.complete, false);

  const result = await read({ uri: databaseUri(path), query: 'SELECT id, v FROM t WHERE id = 1000' });

  assert.deepEqual(result.rows, [source.rows[999]]);
  assert.ok(requests.length > 0);
  assert.ok(requests.length < source.pageCount / 4, `${requests.length} pages out of ${source.pageCount}`);
  assert.equal(new Set(requests).size, requests.length);
  assert.equal(db.missing, source.pageCount - requests.length);
});

test('the background fill completes the database, then reads are local', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const { db, requests } = serve(t, path, source);

  // Missing pages are listed in order
  assert.deepEqual([...db.missingPages()].slice(0, 3), [1, 2, 3]);
  assert.equal(fillAll(db, source), source.pageCount);
  assert.equal(db.complete, true);
  assert.equal(db.missing, 0);
  assert.equal(db.fill(1, source.page(1)), false);
  assert.deepEqual(requests, []);
  assert.ok(readFileSync(path).equals(readFileSync(join(dir, 'source.db'))));

  // The thread which filled the database can read it now
  const local = new DatabaseSync(databaseUri(path));
  assert.deepEqual(local.prepare('SELECT id, v FROM t ORDER BY id').all().map(row => ({ ...row })), source.rows);
  local.close();

  const result = await read({ uri: databaseUri(path), query: 'PRAGMA integrity_check' });
  assert.deepEqual(result.rows, [{ integrity_check: 'ok' }]);
});

test('pages written by SQLite are never overwritten by a fill', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const { db } = serve(t, path, source);

  const result = await read({
    uri: databaseUri(path),
    exec: ['PRAGMA journal_mode = WAL', "UPDATE t SET v = 'changed' WHERE id = 1000", 'PRAGMA wal_checkpoint(TRUNCATE)'],
    query: 'SELECT v FROM t WHERE id = 1000',
  });

  assert.deepEqual(result.rows, [{ v: 'changed' }]);

  // Filling every page with its original version keeps the pages written by the checkpoint
  const written = fillAll(db, source);
  assert.ok(written < source.pageCount);
  assert.equal(db.complete, true);

  const local = new DatabaseSync(databaseUri(path));
  assert.deepEqual({ ...local.prepare('SELECT v FROM t WHERE id = 1000').get() }, { v: 'changed' });
  assert.deepEqual({ ...local.prepare('SELECT v FROM t WHERE id = 1001').get() }, { v: source.rows[1000].v });
  assert.deepEqual({ ...local.prepare('PRAGMA integrity_check').get() }, { integrity_check: 'ok' });
  local.close();
});

test('threads reading the same page share one request', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const { requests } = serve(t, path, source, {}, 50);
  const query = 'SELECT id, v FROM t WHERE id = 1500';

  const results = await Promise.all([read({ uri: databaseUri(path), query }), read({ uri: databaseUri(path), query })]);

  for (const result of results) {
    assert.deepEqual(result.rows, [source.rows[1499]]);
  }

  assert.equal(new Set(requests).size, requests.length);
});

test('a failed fetch fails the read, and the next read requests the page again', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const requests: number[] = [];
  const db = register({ path, pageSize: source.pageSize, pageCount: source.pageCount }, pgno => {
    requests.push(pgno);
    setImmediate(() => (requests.length === 1 ? db.fail(pgno) : db.fill(pgno, source.page(pgno))));
  });
  t.after(() => db.close());

  const failed = await read({ uri: databaseUri(path), query: 'SELECT count(*) AS n FROM t' });
  assert.match(failed.error?.message ?? '', /disk I\/O error/);

  const result = await read({ uri: databaseUri(path), query: 'SELECT count(*) AS n FROM t' });
  assert.deepEqual(result.rows, [{ n: source.rows.length }]);
  assert.equal(requests[0], requests[1]);
});

test('a read fails when the page does not come in time', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const db = register({ path, pageSize: source.pageSize, pageCount: source.pageCount, timeout: 200 }, () => {});
  t.after(() => db.close());

  const start = Date.now();
  const result = await read({ uri: databaseUri(path), query: 'SELECT count(*) FROM t' });

  assert.match(result.error?.message ?? '', /disk I\/O error/);
  assert.ok(Date.now() - start >= 200);
});

test('the thread which fills the database cannot wait for a page', (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const { requests } = serve(t, path, source, { timeout: 10_000 });

  const start = Date.now();

  // node:sqlite reads the first page when it opens the database
  assert.throws(() => new DatabaseSync(databaseUri(path)).prepare('SELECT count(*) FROM t').get(), {
    errcode: 266, // SQLITE_IOERR_READ
  });
  assert.ok(Date.now() - start < 5000);
  assert.deepEqual(requests, []);
});

test('close() fails the reads waiting for a page', async (t) => {
  const dir = tempDir(t);
  const source = createSource(dir);
  const path = join(dir, 'paged.db');
  const options = { path, pageSize: source.pageSize, pageCount: source.pageCount };
  const db = register(options, () => setImmediate(() => db.close()));

  const result = await read({ uri: databaseUri(path), query: 'SELECT count(*) FROM t' });
  assert.match(result.error?.message ?? '', /disk I\/O error/);
  assert.throws(() => db.fill(1, source.page(1)), /closed/);

  // The file can be registered again
  register(options, () => {}).close();
});

test('a file can only be registered once, whatever its path', (t) => {
  const dir = tempDir(t);
  const path = join(dir, 'paged.db');
  const db = register({ path, pageSize: 4096, pageCount: 10 }, () => {});
  t.after(() => db.close());

  symlinkSync(path, join(dir, 'link.db'));

  assert.throws(() => register({ path, pageSize: 4096, pageCount: 10 }, () => {}), /already registered/);
  assert.throws(() => register({ path: join(dir, 'link.db'), pageSize: 4096, pageCount: 10 }, () => {}), /already registered/);
});

test('the page of the lock byte is never missing', (t) => {
  // With 64 KiB pages, the lock byte (at 1 GiB) is in page 16385
  const db = register({ path: join(tempDir(t), 'large.db'), pageSize: 65536, pageCount: 16386 }, () => {});
  t.after(() => db.close());

  assert.equal(db.missing, 16385);
  assert.equal(db.missingPages().includes(16385), false);
});

test('invalid options and pages are rejected', (t) => {
  const path = join(tempDir(t), 'paged.db');

  assert.throws(() => register({ path, pageSize: 1000, pageCount: 1 }, () => {}), /Invalid page size/);
  assert.throws(() => register({ path, pageSize: 4096, pageCount: 0 }, () => {}), /at least one page/);

  const db = register({ path, pageSize: 4096, pageCount: 1 }, () => {});
  t.after(() => db.close());

  assert.throws(() => db.fill(1, new Uint8Array(100)), /100 bytes instead of 4096/);
  assert.equal(db.fill(2, new Uint8Array(4096)), false);
});

test('the library never links nor imports SQLite', (t) => {
  // It must use the SQLite of node:sqlite, through the API table SQLite passes to the extension
  const path = libraryPath();
  const darwin = process.platform === 'darwin';
  const symbols = spawnSync('nm', darwin ? ['-u', path] : ['-D', '--undefined-only', path], { encoding: 'utf8' });

  if (symbols.error) {
    t.skip('nm is not available');
    return;
  }

  assert.equal(symbols.status, 0, symbols.stderr);
  assert.deepEqual(symbols.stdout.split('\n').filter(line => /\bsqlite3_/.test(line)), []);

  const libraries = spawnSync(darwin ? 'otool' : 'ldd', darwin ? ['-L', path] : [path], { encoding: 'utf8' });

  if (!libraries.error) {
    assert.doesNotMatch(libraries.stdout, /libsqlite3/);
  }
});

test('a registered database does not keep the process alive', (t) => {
  const path = join(tempDir(t), 'paged.db');
  const script = `
    import { register } from ${JSON.stringify(new URL('../src/index.ts', import.meta.url).href)};
    register({ path: ${JSON.stringify(path)}, pageSize: 4096, pageCount: 1 }, () => {});
  `;

  const child = spawnSync(process.execPath, ['--input-type=module', '-e', script], { timeout: 10_000, encoding: 'utf8' });

  assert.equal(child.signal, null, 'the process did not exit');
  assert.equal(child.status, 0, child.stderr);
});

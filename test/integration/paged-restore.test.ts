import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Readable } from 'node:stream';

import {
  Decoder,
  ErrIncompleteDatabase,
  FileReplicaClient,
  Replica,
  WALFrameHeaderSize,
  WALHeaderSize,
  type FileInfo,
  type OpenLTXFileOptions,
  type ReplicaClient,
} from '../../src/index.ts';
import { LocalDatabase } from '../../src/db.ts';
import { loadPagedVfs, type PagedVfs } from '../../src/paged-restore.ts';
import { makeTempDir } from '../helpers/make-db.ts';

/**
 * A paged restore makes the database usable right away: queries from other
 * threads wait for the pages they read, while the others are filled in the
 * background, and replication goes on meanwhile.
 */

let vfs: PagedVfs | null = null;
try {
  vfs = await loadPagedVfs();
} catch {
  // Build it with pnpm --filter @platformatic/sqlite-js-stream-paged-vfs build:native build
}
const skip = vfs === null ? 'the paged VFS is not built' : false;

const Rows = 2000;
const Tables = 80;

/**
 * A database of about `rows` pages, whose schema spans several pages and has
 * records larger than a page (so overflow pages), replicated as a level-9
 * snapshot followed by level-0 files which change some of its pages.
 */
async function replicate(t: TestContext, client: ReplicaClient, rows = Rows): Promise<DatabaseSync> {
  const path = join(makeTempDir('paged-src-'), 'src.db');
  const src = new DatabaseSync(path);
  t.after(() => src.close());
  src.exec('PRAGMA journal_mode=WAL');
  src.exec('PRAGMA wal_autocheckpoint=0');
  src.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)');
  src.exec('BEGIN');
  for (let i = 0; i < Tables; i++) src.exec(`CREATE TABLE extra_${i} (a INTEGER, b TEXT, c BLOB, d REAL)`);
  const columns = Array.from({ length: 400 }, (_, i) => `column_with_a_long_name_${i} TEXT`).join(', ');
  src.exec(`CREATE TABLE wide (${columns})`);
  src.exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${rows}) INSERT INTO t SELECT i, randomblob(3000) FROM n`);
  src.exec('COMMIT');

  const replica = new Replica(path, client);
  t.after(() => replica.close());
  await replica.sync();
  for (let i = 0; i < 3; i++) {
    src.exec(`UPDATE t SET v = randomblob(3000) WHERE id % 97 = ${i}`);
    await replica.sync();
  }
  await replica.snapshot();
  for (let i = 3; i < 5; i++) {
    src.exec(`UPDATE t SET v = randomblob(3000) WHERE id % 97 = ${i}`);
    src.exec(`INSERT INTO t (v) VALUES (randomblob(3000))`);
    await replica.sync();
  }
  return src;
}

/**
 * A client whose ranged reads of at least `minSize` bytes (those of the
 * background fill, not those for the pages queries wait for) wait for
 * `release()`. It records the reads, and counts the bytes they return.
 */
function gated(
  client: ReplicaClient,
  minSize = 256 * 1024,
): { client: ReplicaClient; release: () => void; reads: OpenLTXFileOptions[]; bytes: () => number } {
  let release!: () => void;
  const open = new Promise<void>((resolve) => {
    release = resolve;
  });
  const reads: OpenLTXFileOptions[] = [];
  let bytes = 0;
  const proxy = new Proxy(client, {
    get(target, prop) {
      if (prop === 'openLtxFile') {
        return async (opts: OpenLTXFileOptions) => {
          reads.push(opts);
          if (opts.size >= minSize) await open;
          const stream = await target.openLtxFile(opts);
          return Readable.from(
            (async function* () {
              for await (const chunk of stream) {
                bytes += chunk.length;
                yield chunk;
              }
            })(),
          );
        };
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  return { client: proxy, release, reads, bytes: () => bytes };
}

interface SqlWorker {
  all(sql: string, ...params: unknown[]): Promise<Array<Record<string, unknown>>>;
  exec(sql: string): Promise<void>;
  close(): Promise<void>;
}

function sqlWorker(t: TestContext, path: string, writer = false): SqlWorker {
  const worker = new Worker(new URL('../helpers/sql-worker.ts', import.meta.url), {
    workerData: { uri: vfs!.databaseUri(path), writer },
  });
  t.after(() => worker.terminate());
  let next = 0;
  const call = (message: object): Promise<{ rows?: Array<Record<string, unknown>> }> => {
    const id = next++;
    return new Promise((resolve, reject) => {
      const onMessage = (m: { id: number; rows?: Array<Record<string, unknown>>; error?: string }) => {
        if (m.id !== id) return;
        worker.off('message', onMessage);
        if (m.error !== undefined) reject(new Error(m.error));
        else resolve(m);
      };
      worker.on('message', onMessage);
      worker.postMessage({ id, ...message });
    });
  };
  return {
    all: async (sql, ...params) => (await call({ sql, params })).rows!,
    exec: async (exec) => {
      await call({ exec });
    },
    close: async () => {
      await call({ close: true });
    },
  };
}

/** Rows as plain objects, as they come from a worker. */
function plain(rows: unknown[]): Array<Record<string, unknown>> {
  return rows.map((row) => ({ ...(row as Record<string, unknown>) }));
}

async function exists(client: ReplicaClient, info: FileInfo): Promise<boolean> {
  for await (const f of client.ltxFiles({ level: info.level, seek: 0n, useMetadata: false })) {
    if (f.minTXID === info.minTXID && f.maxTXID === info.maxTXID) return true;
  }
  return false;
}

function integrity(path: string): string {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    return (db.prepare('PRAGMA integrity_check').get() as { integrity_check: string }).integrity_check;
  } finally {
    db.close();
  }
}

test('a paged restore serves queries before the database is downloaded', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  const src = await replicate(t, backing);
  const { client, release, reads } = gated(backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  const misses: number[] = [];
  const result = await replica.restorePaged({ readAhead: 8, onMiss: (pgno) => misses.push(pgno) });
  assert.equal(result.paged, true);
  assert.equal(result.unavailable, null);
  assert.equal(result.txID, 6n);
  assert.ok(existsSync(join(`${path}-litestream`, 'paged')), 'the marker exists while the database is incomplete');

  // The background fill is held, yet queries are answered
  const db = sqlWorker(t, path);
  const want = plain(src.prepare('SELECT id, v FROM t WHERE id IN (1, 777, 1500, 2001, 2002) ORDER BY id').all());
  assert.deepEqual(await db.all('SELECT id, v FROM t WHERE id IN (1, 777, 1500, 2001, 2002) ORDER BY id'), want);
  assert.deepEqual(
    await db.all("SELECT count(*) AS n FROM sqlite_master WHERE type = 'table'"),
    [{ n: Tables + 2 }],
  );
  assert.ok(misses.length > 0, 'queries waited for pages');
  assert.ok(reads.some((r) => r.size >= 256 * 1024), 'the background fill is waiting');

  release();
  await result.filled;
  assert.equal(existsSync(join(`${path}-litestream`, 'paged')), false, 'the marker is removed once complete');
  assert.deepEqual(
    await db.all('SELECT count(*) AS n, sum(length(v)) AS size FROM t'),
    plain(src.prepare('SELECT count(*) AS n, sum(length(v)) AS size FROM t').all()),
  );
  await db.close();

  // Page for page, the database is the one a full restore writes
  const out = join(makeTempDir('paged-full-'), 'db.db');
  await new Replica(join(makeTempDir('paged-other-'), 'x.db'), backing).restore({ outputPath: out });
  assert.deepEqual(await readFile(path), await readFile(out));
  assert.equal(integrity(path), 'ok');
});

test('replication goes on while a paged restore fills the database', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  const src = await replicate(t, backing);
  const { client, release } = gated(backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  const result = await replica.restorePaged();

  const db = sqlWorker(t, path, true);
  await db.exec("UPDATE t SET v = x'00' WHERE id = 10; INSERT INTO t (v) VALUES (x'01')");

  // The first sync is incremental, from the WAL the restore wrote
  const info = await replica.sync();
  assert.equal(info?.maxTXID, 7n);
  const local = await LocalDatabase.open(path);
  t.after(() => local.close());
  const dec = new Decoder(new Uint8Array(await readFile(local.ltxPath(0, 7n, 7n))));
  dec.decodeHeader();
  assert.equal(dec.Header().walOffset, BigInt(WALHeaderSize + WALFrameHeaderSize + 4096));

  // Retention keeps the files the restore reads (the snapshot at TXID 4 and
  // the level-0 files after it), until it is done
  const l0 = (txid: bigint): FileInfo => ({ level: 0, minTXID: txid, maxTXID: txid, size: 0, createdAt: 0 });
  assert.equal(await replica.enforceRetentionByTXID(0, 7n), 4);
  assert.equal(await exists(backing, l0(1n)), false);
  assert.equal(await exists(backing, l0(5n)), true);

  // A snapshot reads every page: the missing ones are read first, or when the
  // background fill, held until now, reads them
  const snapshot = replica.snapshot();
  release();
  assert.equal((await snapshot)?.maxTXID, 7n);

  await result.filled;
  assert.equal(await replica.enforceRetentionByTXID(0, 7n), 2);
  assert.equal(await exists(backing, l0(5n)), false, 'deleted once the restore is done');

  // The replica has the changes made during the restore
  const out = join(makeTempDir('paged-check-'), 'db.db');
  await new Replica(join(makeTempDir('paged-other-'), 'x.db'), backing).restore({ outputPath: out });
  const check = new DatabaseSync(out, { readOnly: true });
  t.after(() => check.close());
  assert.deepEqual(plain(check.prepare('SELECT v FROM t WHERE id = 10').all()), [{ v: new Uint8Array([0]) }]);
  const before = (src.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
  assert.deepEqual(plain(check.prepare('SELECT count(*) AS n FROM t').all()), [{ n: before + 1 }]);
  await db.close();
});

test('once every page is restored, nothing is read from the replica', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  const src = await replicate(t, backing);
  const { client, release, reads } = gated(backing);
  release();

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  const result = await replica.restorePaged();
  await result.filled;
  reads.length = 0;

  // Queries read every page, writes are replicated, and a snapshot reads the database again
  const db = sqlWorker(t, path, true);
  assert.deepEqual(await db.all('PRAGMA integrity_check'), [{ integrity_check: 'ok' }]);
  assert.deepEqual(
    await db.all('SELECT count(*) AS n, sum(length(v)) AS size FROM t'),
    plain(src.prepare('SELECT count(*) AS n, sum(length(v)) AS size FROM t').all()),
  );
  await db.exec("INSERT INTO t (v) VALUES (x'02')");
  assert.equal((await replica.sync())?.maxTXID, 7n);
  assert.equal((await replica.snapshot())?.maxTXID, 7n);
  await db.close();

  assert.deepEqual(reads, []);
});

test('the reads before a paged restore resolves barely grow with the database (acceptance)', { skip }, async (t) => {
  // What is read before the database can be opened: the headers of two files, the page indexes of the plan (a few
  // bytes per page), and the schema with its read-ahead. Queries then read only the pages they need, so the time to
  // the first response does not grow with the size of the database the way a full restore does.
  const measure = async (rows: number): Promise<{ size: number; before: number; reads: number }> => {
    const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
    await replicate(t, backing, rows);
    const { client, release, bytes, reads } = gated(backing);
    t.after(release);

    const path = join(makeTempDir('paged-dst-'), 'db.db');
    const replica = new Replica(path, client);
    t.after(() => replica.close());
    const result = await replica.restorePaged();
    const before = { before: bytes(), reads: reads.length };
    release();
    await result.filled;
    return { size: (await readFile(path)).length, ...before };
  };

  const small = await measure(2000);
  const large = await measure(8000);
  assert.ok(large.size > 3.5 * small.size, `the large database (${large.size} bytes) is 4 times the small one (${small.size})`);
  assert.ok(large.before < 2 * small.before, `read before opening: ${large.before} bytes for ${large.size}, ${small.before} for ${small.size}`);
  assert.ok(large.before < large.size / 50, `read before opening: ${large.before} bytes, under 2% of ${large.size}`);
  t.diagnostic(`read before opening: ${small.before} of ${small.size} bytes, ${large.before} of ${large.size} bytes`);
});

test('closing the replica stops a paged restore, and the database is restored again', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  await replicate(t, backing);
  const { client } = gated(backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, client);
  const result = await replica.restorePaged();
  replica.close();
  await assert.rejects(result.filled, { code: 'ERR_PAGED_RESTORE_CLOSED' });

  // As after a crash: the database is incomplete
  const next = new Replica(path, backing);
  t.after(() => next.close());
  assert.equal(await next.localPos(), 0n);
  await assert.rejects(next.sync(), ErrIncompleteDatabase);
  await assert.rejects(next.snapshot(), ErrIncompleteDatabase);

  await next.ensureExists();
  assert.equal(await next.localPos(), 6n);
  assert.equal(existsSync(join(`${path}-litestream`, 'paged')), false);
  assert.equal(integrity(path), 'ok');
});

test('a database left incomplete by a crash during a paged restore is restored again', { skip }, async (t) => {
  const dir = makeTempDir('paged-repl-');
  const backing = new FileReplicaClient(dir);
  await replicate(t, backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const child = spawn(process.execPath, [new URL('../helpers/paged-restore-child.ts', import.meta.url).pathname, path, dir], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  t.after(() => child.kill('SIGKILL'));
  const [ready] = await once(child.stdout!, 'data');
  assert.equal(String(ready), 'ready\n');
  child.kill('SIGKILL');
  await once(child, 'exit');

  const replica = new Replica(path, backing);
  t.after(() => replica.close());
  assert.equal(await replica.localPos(), 0n);
  await assert.rejects(replica.sync(), ErrIncompleteDatabase);

  // A paged restore again, this time to the end
  const result = await replica.restorePaged();
  await result.filled;
  assert.equal(await replica.localPos(), 6n);
  assert.equal(integrity(path), 'ok');
});

test('a paged restore restores fully a database smaller than minSize', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  await replicate(t, backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, backing);
  t.after(() => replica.close());
  const result = await replica.restorePaged({ minSize: Number.MAX_SAFE_INTEGER });
  assert.equal(result.paged, false);
  assert.equal(result.unavailable, null);
  assert.ok(result.size > 0);
  await result.filled;
  assert.equal(existsSync(join(`${path}-litestream`, 'paged')), false);
  assert.equal(await replica.localPos(), 6n);
  assert.equal(integrity(path), 'ok');
});

test('a full restore completes a database a paged restore left incomplete', { skip }, async (t) => {
  const backing = new FileReplicaClient(makeTempDir('paged-repl-'));
  await replicate(t, backing);
  const { client } = gated(backing);

  const path = join(makeTempDir('paged-dst-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  const result = await replica.restorePaged();

  // Restoring again stops the paged restore
  await replica.restore({ outputPath: path });
  await assert.rejects(result.filled, { code: 'ERR_PAGED_RESTORE_CLOSED' });
  assert.equal(existsSync(join(`${path}-litestream`, 'paged')), false);
  assert.equal(await replica.localPos(), 6n);
  assert.equal(integrity(path), 'ok');
});

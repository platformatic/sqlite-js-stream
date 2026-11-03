import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  Decoder,
  Replica,
  FileReplicaClient,
  WALHeaderSize,
  runCheckpoint,
  type CheckpointToken,
} from '../../src/index.ts';
import { LocalDatabase } from '../../src/db.ts';
import { createWALDB, makeTempDir, type TestDB } from '../helpers/make-db.ts';

/**
 * `checkpoint()` resets the WAL right after a sync and records it, so the next
 * sync continues incrementally from the start of the new WAL instead of
 * writing a full snapshot. Any other WAL reset still produces a snapshot.
 */

interface LTXSummary {
  walOffset: bigint;
  commit: number;
  pages: number;
}

async function summarize(db: LocalDatabase, txID: bigint): Promise<LTXSummary> {
  const dec = new Decoder(new Uint8Array(await readFile(db.ltxPath(0, txID, txID))));
  dec.decodeHeader();
  const h = dec.Header();
  const data = new Uint8Array(h.pageSize);
  let pages = 0;
  while (dec.decodePage(data) !== null) pages++;
  dec.close();
  return { walOffset: h.walOffset, commit: h.commit, pages };
}

/** Inserts enough rows to make the database a few dozen pages. */
function fill(src: TestDB, n: number, prefix: string): void {
  const insert = src.db.prepare('INSERT INTO t (name) VALUES (?)');
  src.db.exec('BEGIN');
  for (let i = 0; i < n; i++) insert.run(`${prefix}-${i}-${'x'.repeat(200)}`);
  src.db.exec('COMMIT');
}

async function assertRestoreMatches(t: TestContext, replica: Replica, src: TestDB): Promise<void> {
  const out = join(makeTempDir('restore-'), 'db.db');
  await replica.restore({ outputPath: out });
  const r = new DatabaseSync(out);
  t.after(() => r.close());
  const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
  assert.equal(ic.integrity_check, 'ok');
  const got = r.prepare('SELECT id, name FROM t ORDER BY id').all();
  const want = src.db.prepare('SELECT id, name FROM t ORDER BY id').all();
  assert.deepEqual(got, want);

  // Page for page, the restored file equals the source database.
  src.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  assert.deepEqual(await readFile(out), await readFile(src.path));
}

test('sync stays incremental after checkpoint(TRUNCATE)', async (t) => {
  const src = createWALDB(['a', 'b', 'c']);
  t.after(() => src.close());
  fill(src, 200, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  const base = await replica.sync();
  assert.equal(base?.maxTXID, 1n);
  const snap = await summarize(db, 1n);

  assert.equal(await replica.checkpoint(src.db, 'TRUNCATE'), true);
  assert.equal((await readFile(src.walPath)).length, 0, 'the WAL is truncated');

  // Nothing was written since the checkpoint: nothing to sync.
  assert.equal(await replica.sync(), null);

  src.db.exec("INSERT INTO t (name) VALUES ('after')");
  const info = await replica.sync();
  assert.equal(info?.maxTXID, 2n);

  const inc = await summarize(db, 2n);
  assert.equal(inc.walOffset, BigInt(WALHeaderSize), 'continues from the start of the new WAL');
  assert.ok(inc.pages < snap.pages / 4, `incremental (${inc.pages} pages) is not a snapshot (${snap.pages})`);
  assert.ok(info!.size < base!.size / 4, 'incremental file is small');

  await assertRestoreMatches(t, replica, src);
});

test('prepareCheckpoint in a replication thread, runCheckpoint on the writer', async (t) => {
  const src = createWALDB(['a']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const clientPath = makeTempDir('repl-');
  const worker = new Worker(new URL('../helpers/replication-worker.ts', import.meta.url));
  t.after(() => worker.terminate());
  let nextId = 0;
  const call = (type: string, extra: object = {}): Promise<unknown> => {
    const id = nextId++;
    return new Promise((resolve, reject) => {
      const onMessage = (m: { id: number; result?: unknown; error?: string }) => {
        if (m.id !== id) return;
        worker.off('message', onMessage);
        if (m.error !== undefined) reject(new Error(m.error));
        else resolve(m.result);
      };
      worker.on('message', onMessage);
      worker.postMessage({ id, type, ...extra });
    });
  };
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  await call('open', { dbPath: src.path, clientPath });
  await call('sync');
  const snap = await summarize(db, 1n);

  for (let i = 0; i < 3; i++) {
    // The token crosses threads as plain data.
    const token = (await call('prepareCheckpoint')) as CheckpointToken;
    assert.deepEqual(JSON.parse(JSON.stringify(token)), token);
    assert.equal(runCheckpoint(src.db, token, 'TRUNCATE'), true);

    src.db.exec(`UPDATE t SET name = 'upd-${i}' WHERE id = ${i + 1}`);
    fill(src, 5, `round-${i}`);
    // The worker's Replica (and its cached positions) picks up the record.
    assert.deepEqual(await call('sync'), { level: 0, maxTXID: String(i + 2) });
  }

  const files = await db.l0Files();
  assert.deepEqual(files.map((f) => f.maxTXID), [1n, 2n, 3n, 4n]);
  for (const f of files.slice(1)) {
    const s = await summarize(db, f.maxTXID);
    assert.equal(s.walOffset, BigInt(WALHeaderSize));
    assert.ok(s.pages < snap.pages, `TXID ${f.maxTXID} is incremental (${s.pages} pages)`);
  }

  await call('close');
  // A separate Replica, sharing nothing with the worker, restores the data.
  const reader = new Replica(src.path, new FileReplicaClient(clientPath));
  t.after(() => reader.close());
  await assertRestoreMatches(t, reader, src);
});

test('restore is correct across several checkpoints', async (t) => {
  const src = createWALDB(['a']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  await replica.sync();
  const snap = await summarize(db, 1n);

  for (let i = 0; i < 5; i++) {
    assert.equal(await replica.checkpoint(src.db, i % 2 === 0 ? 'TRUNCATE' : 'RESTART'), true);
    src.db.exec(`UPDATE t SET name = 'upd-${i}' WHERE id = ${i + 1}`);
    fill(src, 10, `round-${i}`);
    await replica.sync();
  }

  const files = await db.l0Files();
  assert.deepEqual(files.map((f) => f.maxTXID), [1n, 2n, 3n, 4n, 5n, 6n]);
  for (const f of files.slice(1)) {
    const s = await summarize(db, f.maxTXID);
    assert.equal(s.walOffset, BigInt(WALHeaderSize));
    assert.ok(s.pages < snap.pages, `TXID ${f.maxTXID} is incremental (${s.pages} pages)`);
  }

  await assertRestoreMatches(t, replica, src);
});

test('checkpoint(RESTART) ignores frames of the old WAL left on disk', async (t) => {
  const src = createWALDB(['a']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  await replica.sync();
  assert.equal(await replica.checkpoint(src.db, 'RESTART'), true);

  // The new WAL is much shorter than the old one, whose frames stay on disk.
  src.db.exec("INSERT INTO t (name) VALUES ('after')");
  const info = await replica.sync();
  assert.equal(info?.maxTXID, 2n);

  await assertRestoreMatches(t, replica, src);
});

test('a WAL started by another connection falls back to a snapshot', async (t) => {
  const src = createWALDB(['a', 'b']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  const other = new DatabaseSync(src.path);
  t.after(() => other.close());
  await replica.sync();
  // The checkpoint runs on a connection that does not write: the writer,
  // which never reset the WAL, starts the next one with random salts.
  assert.equal(await replica.checkpoint(other, 'TRUNCATE'), true);
  src.db.exec("INSERT INTO t (name) VALUES ('after')");
  await replica.sync();

  const s = await summarize(db, 2n);
  assert.equal(s.pages, s.commit, 'the next file is a full snapshot');
  await assertRestoreMatches(t, replica, src);
});

for (const mode of ['RESTART', 'TRUNCATE']) {
  test(`a checkpoint run directly with PRAGMA wal_checkpoint(${mode}) still produces a snapshot`, async (t) => {
    const src = createWALDB(['a', 'b']);
    t.after(() => src.close());
    fill(src, 100, 'base');
    const client = new FileReplicaClient(makeTempDir('repl-'));
    const replica = new Replica(src.path, client);
    t.after(() => replica.close());
    const db = await LocalDatabase.open(src.path);
    t.after(() => db.close());
    await replica.sync();
    src.db.prepare(`PRAGMA wal_checkpoint(${mode})`).get();
    // With RESTART, the frames of the old WAL stay on disk past the new one.
    src.db.exec("INSERT INTO t (name) VALUES ('after')");
    const info = await replica.sync();
    assert.equal(info?.maxTXID, 2n, 'the new WAL is not missed');

    const s = await summarize(db, 2n);
    assert.equal(s.pages, s.commit, 'the next file is a full snapshot');
    await assertRestoreMatches(t, replica, src);
  });
}

test('a commit between the sync and the checkpoint produces a snapshot', async (t) => {
  const src = createWALDB(['a', 'b']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  await db.sync();
  // Not synced: the checkpoint would move it into the database file.
  src.db.exec("INSERT INTO t (name) VALUES ('unsynced')");
  assert.equal(runCheckpoint(src.db, await db.checkpointToken(), 'TRUNCATE'), false);

  src.db.exec("INSERT INTO t (name) VALUES ('after')");
  await db.sync();
  const s = await summarize(db, 2n);
  assert.equal(s.pages, s.commit, 'the next file is a full snapshot');

  await replica.sync();
  await assertRestoreMatches(t, replica, src);
});

test('a busy checkpoint leaves the next sync incremental in the old WAL', async (t) => {
  const src = createWALDB(['a', 'b']);
  t.after(() => src.close());
  fill(src, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(src.path, client);
  t.after(() => replica.close());
  const db = await LocalDatabase.open(src.path);
  t.after(() => db.close());
  const reader = new DatabaseSync(src.path, { readOnly: true });
  t.after(() => reader.close());
  await replica.sync();

  // A reader pinned on the WAL keeps RESTART from completing.
  reader.exec('BEGIN');
  reader.prepare('SELECT count(*) FROM t').get();
  src.db.exec('PRAGMA busy_timeout=0');
  assert.equal(await replica.checkpoint(src.db, 'TRUNCATE'), false);

  src.db.exec("INSERT INTO t (name) VALUES ('after')");
  await replica.sync();
  const s = await summarize(db, 2n);
  assert.ok(s.walOffset > BigInt(WALHeaderSize), 'resumes in the old WAL');
  assert.ok(s.pages < s.commit, 'the next file is incremental');

  reader.exec('COMMIT');
  await assertRestoreMatches(t, replica, src);
});

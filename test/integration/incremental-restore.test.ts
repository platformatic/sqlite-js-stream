import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { Decoder, FileReplicaClient, Replica, WALFrameHeaderSize, WALHeaderSize } from '../../src/index.ts';
import { LocalDatabase } from '../../src/db.ts';
import { createWALDB, makeTempDir, type TestDB } from '../helpers/make-db.ts';

/**
 * A restore into the replicated database's own path writes the first WAL of
 * the restored database, so the first sync after it uploads only the changes
 * made since, instead of a snapshot of the whole database. Any reset of that
 * WAL still produces a snapshot.
 */

interface LTXSummary {
  walOffset: bigint;
  commit: number;
  pages: number;
}

async function summarize(path: string, txID: bigint): Promise<LTXSummary> {
  const db = await LocalDatabase.open(path);
  try {
    const dec = new Decoder(new Uint8Array(await readFile(db.ltxPath(0, txID, txID))));
    dec.decodeHeader();
    const h = dec.Header();
    const data = new Uint8Array(h.pageSize);
    let pages = 0;
    while (dec.decodePage(data) !== null) pages++;
    dec.close();
    return { walOffset: h.walOffset, commit: h.commit, pages };
  } finally {
    db.close();
  }
}

/** Inserts enough rows to make the database a few dozen pages. */
function fill(db: DatabaseSync, n: number, prefix: string): void {
  const insert = db.prepare('INSERT INTO t (name) VALUES (?)');
  db.exec('BEGIN');
  for (let i = 0; i < n; i++) insert.run(`${prefix}-${i}-${'x'.repeat(200)}`);
  db.exec('COMMIT');
}

function openWriter(t: TestContext, path: string): DatabaseSync {
  const conn = new DatabaseSync(path);
  conn.exec('PRAGMA journal_mode=WAL');
  conn.exec('PRAGMA wal_autocheckpoint=0');
  t.after(() => {
    if (conn.isOpen) conn.close();
  });
  return conn;
}

interface Restored {
  src: TestDB;
  client: FileReplicaClient;
  /** The replica of the restored database. */
  replica: Replica;
  path: string;
  /** TXID of the replica when it was restored. */
  txid: bigint;
}

/** Replicates a database of a few dozen pages, and restores it into a new path. */
async function replicateAndRestore(t: TestContext): Promise<Restored> {
  const src = createWALDB(['a', 'b']);
  t.after(() => src.close());
  fill(src.db, 200, 'base');

  const client = new FileReplicaClient(makeTempDir('repl-'));
  const first = new Replica(src.path, client);
  t.after(() => first.close());
  await first.sync();
  src.db.exec("INSERT INTO t (name) VALUES ('c')");
  await first.sync();

  const path = join(makeTempDir('restored-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  await replica.restore({ outputPath: path });

  return { src, client, replica, path, txid: 2n };
}

/** The replica restores to the content of the database `conn` writes. */
async function assertRestoreMatches(t: TestContext, client: FileReplicaClient, conn: DatabaseSync): Promise<void> {
  const dir = makeTempDir('check-');
  const out = join(dir, 'db.db');
  const reader = new Replica(join(dir, 'replicated.db'), client);
  t.after(() => reader.close());
  await reader.restore({ outputPath: out });

  const r = new DatabaseSync(out);
  t.after(() => r.close());
  const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
  assert.equal(ic.integrity_check, 'ok');
  const got = r.prepare('SELECT id, name FROM t ORDER BY id').all();
  const want = conn.prepare('SELECT id, name FROM t ORDER BY id').all();
  assert.deepEqual(got, want);
}

test('the first sync after a restore is incremental', async (t) => {
  const { client, replica, path, txid } = await replicateAndRestore(t);
  const conn = openWriter(t, path);

  // Nothing was written since the restore: nothing to sync.
  assert.equal(await replica.sync(), null);

  conn.exec("UPDATE t SET name = 'updated' WHERE id = 1");
  fill(conn, 5, 'after');
  const info = await replica.sync();
  assert.equal(info?.maxTXID, txid + 1n);

  const s = await summarize(path, txid + 1n);
  const frameSize = BigInt(WALFrameHeaderSize + 4096);
  assert.equal(s.walOffset, BigInt(WALHeaderSize) + frameSize, 'resumes after the frame written by the restore');
  assert.ok(s.pages < s.commit / 4, `incremental (${s.pages} pages of ${s.commit})`);

  conn.exec("INSERT INTO t (name) VALUES ('later')");
  assert.equal((await replica.sync())?.maxTXID, txid + 2n);
  assert.ok((await summarize(path, txid + 2n)).pages < s.commit / 4);

  await assertRestoreMatches(t, client, conn);
});

for (const pageSize of [512, 65536]) {
  test(`the first sync after a restore is incremental with ${pageSize}-byte pages`, async (t) => {
    const dir = makeTempDir('pages-');
    const src = new DatabaseSync(join(dir, 'src.db'));
    t.after(() => src.close());
    src.exec(`PRAGMA page_size=${pageSize}`);
    src.exec('PRAGMA journal_mode=WAL');
    src.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    fill(src, pageSize === 512 ? 200 : 2000, 'base');

    const client = new FileReplicaClient(join(dir, 'repl'));
    const first = new Replica(join(dir, 'src.db'), client);
    t.after(() => first.close());
    await first.sync();

    const path = join(dir, 'restored.db');
    const replica = new Replica(path, client);
    t.after(() => replica.close());
    await replica.restore({ outputPath: path });

    const conn = openWriter(t, path);
    conn.exec("UPDATE t SET name = 'updated' WHERE id = 1");
    await replica.sync();

    const s = await summarize(path, 2n);
    assert.equal(s.walOffset, BigInt(WALHeaderSize + WALFrameHeaderSize + pageSize));
    assert.ok(s.pages < s.commit / 4, `incremental (${s.pages} pages of ${s.commit})`);
    await assertRestoreMatches(t, client, conn);
  });
}

test('a checkpoint which does not reset the WAL keeps the first sync incremental', async (t) => {
  const { client, replica, path, txid } = await replicateAndRestore(t);
  const conn = openWriter(t, path);
  const reader = new DatabaseSync(path, { readOnly: true });
  t.after(() => reader.close());

  fill(conn, 20, 'before');
  // A reader keeps the checkpoint from copying the last frames, so the WAL is
  // not reset by the next write.
  reader.exec('BEGIN');
  reader.prepare('SELECT count(*) FROM t').get();
  fill(conn, 5, 'pinned');
  const r = conn.prepare('PRAGMA wal_checkpoint(PASSIVE)').get() as { log: number; checkpointed: number };
  assert.ok(r.checkpointed > 0 && r.checkpointed < r.log, `a partial checkpoint (${r.checkpointed} of ${r.log})`);
  fill(conn, 5, 'after');
  reader.exec('COMMIT');

  await replica.sync();
  const s = await summarize(path, txid + 1n);
  assert.ok(s.pages < s.commit / 2, `incremental (${s.pages} pages of ${s.commit})`);
  await assertRestoreMatches(t, client, conn);
});

test('checkpoint() right after a restore keeps the next sync incremental', async (t) => {
  const { client, replica, path, txid } = await replicateAndRestore(t);
  const conn = openWriter(t, path);

  assert.equal(await replica.checkpoint(conn, 'TRUNCATE'), true);
  conn.exec("UPDATE t SET name = 'updated' WHERE id = 1");
  await replica.sync();

  const s = await summarize(path, txid + 1n);
  assert.equal(s.walOffset, BigInt(WALHeaderSize), 'continues from the start of the new WAL');
  assert.ok(s.pages < s.commit / 4, `incremental (${s.pages} pages of ${s.commit})`);
  await assertRestoreMatches(t, client, conn);
});

const resets: Array<[string, (t: TestContext, path: string, conn: DatabaseSync) => DatabaseSync]> = [
  [
    'a checkpoint restarts the WAL',
    (_t, _path, conn) => {
      conn.prepare('PRAGMA wal_checkpoint(RESTART)').get();
      return conn;
    },
  ],
  [
    'a checkpoint truncates the WAL and another connection writes next',
    (t, path, conn) => {
      conn.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
      return openWriter(t, path);
    },
  ],
  [
    'the last connection closes, which deletes the WAL',
    (t, path, conn) => {
      conn.close();
      assert.equal(existsSync(`${path}-wal`), false);
      return openWriter(t, path);
    },
  ],
];

for (const [name, reset] of resets) {
  test(`the first sync after a restore is a snapshot when ${name}`, async (t) => {
    const { client, replica, path, txid } = await replicateAndRestore(t);
    let conn = openWriter(t, path);

    fill(conn, 5, 'before');
    conn = reset(t, path, conn);
    fill(conn, 5, 'after');

    await replica.sync();
    const s = await summarize(path, txid + 1n);
    assert.equal(s.pages, s.commit, 'a full snapshot');
    await assertRestoreMatches(t, client, conn);
  });
}

test('a restore records nothing for a database which is not in WAL mode', async (t) => {
  const src = createWALDB(['a', 'b']);
  t.after(() => src.close());
  src.db.exec('PRAGMA journal_mode=DELETE');
  fill(src.db, 100, 'base');

  const client = new FileReplicaClient(makeTempDir('repl-'));
  const first = new Replica(src.path, client);
  t.after(() => first.close());
  await first.sync();

  const path = join(makeTempDir('restored-'), 'db.db');
  const replica = new Replica(path, client);
  t.after(() => replica.close());
  await replica.restore({ outputPath: path });
  assert.equal(existsSync(`${path}-wal`), false);

  const conn = new DatabaseSync(path);
  t.after(() => conn.close());
  conn.exec("INSERT INTO t (name) VALUES ('after')");
  await replica.sync();
  const s = await summarize(path, 2n);
  assert.equal(s.pages, s.commit, 'a full snapshot');
});

test('a restore into another path writes no WAL', async (t) => {
  const { client } = await replicateAndRestore(t);
  const out = join(makeTempDir('other-'), 'db.db');
  const replica = new Replica(join(makeTempDir('other-'), 'replicated.db'), client);
  t.after(() => replica.close());

  await replica.restore({ outputPath: out });
  assert.equal(existsSync(`${out}-wal`), false);
});

test('a restore record of an older restore is ignored', async (t) => {
  const { client, replica, path, txid } = await replicateAndRestore(t);
  const conn = openWriter(t, path);
  fill(conn, 5, 'after');

  // As if a crash left the record of a restore at another TXID
  const db = await LocalDatabase.open(path);
  const record = JSON.parse(await readFile(db.restorePath(), 'utf8'));
  await writeFile(db.restorePath(), JSON.stringify({ ...record, txid: String(txid - 1n) }));
  db.close();

  await replica.sync();
  const s = await summarize(path, txid + 1n);
  assert.equal(s.pages, s.commit, 'a full snapshot');
  await assertRestoreMatches(t, client, conn);
});

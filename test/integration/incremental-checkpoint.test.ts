import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { Decoder, Replica, FileReplicaClient, WALHeaderSize } from '../../src/index.ts';
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
function fill(t: TestDB, n: number, prefix: string): void {
  const insert = t.db.prepare('INSERT INTO t (name) VALUES (?)');
  t.db.exec('BEGIN');
  for (let i = 0; i < n; i++) insert.run(`${prefix}-${i}-${'x'.repeat(200)}`);
  t.db.exec('COMMIT');
}

async function assertRestoreMatches(replica: Replica, t: TestDB): Promise<void> {
  const out = join(makeTempDir('restore-'), 'db.db');
  await replica.restore({ outputPath: out });
  const r = new DatabaseSync(out);
  try {
    const ic = r.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    const got = r.prepare('SELECT id, name FROM t ORDER BY id').all();
    const want = t.db.prepare('SELECT id, name FROM t ORDER BY id').all();
    assert.deepEqual(got, want);
  } finally {
    r.close();
  }

  // Page for page, the restored file equals the source database.
  t.db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  assert.deepEqual(await readFile(out), await readFile(t.path));
}

test('sync stays incremental after checkpoint(TRUNCATE)', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  fill(t, 200, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const db = await LocalDatabase.open(t.path);
  try {
    const base = await replica.sync();
    assert.equal(base?.maxTXID, 1n);
    const snap = await summarize(db, 1n);

    assert.equal(await replica.checkpoint(t.db, 'TRUNCATE'), true);
    assert.equal((await readFile(t.walPath)).length, 0, 'the WAL is truncated');

    // Nothing was written since the checkpoint: nothing to sync.
    assert.equal(await replica.sync(), null);

    t.db.exec("INSERT INTO t (name) VALUES ('after')");
    const info = await replica.sync();
    assert.equal(info?.maxTXID, 2n);

    const inc = await summarize(db, 2n);
    assert.equal(inc.walOffset, BigInt(WALHeaderSize), 'continues from the start of the new WAL');
    assert.ok(inc.pages < snap.pages / 4, `incremental (${inc.pages} pages) is not a snapshot (${snap.pages})`);
    assert.ok(info!.size < base!.size / 4, 'incremental file is small');

    await assertRestoreMatches(replica, t);
  } finally {
    db.close();
    replica.close();
    t.close();
  }
});

test('restore is correct across several checkpoints', async () => {
  const t = createWALDB(['a']);
  fill(t, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const db = await LocalDatabase.open(t.path);
  try {
    await replica.sync();
    const snap = await summarize(db, 1n);

    for (let i = 0; i < 5; i++) {
      assert.equal(await replica.checkpoint(t.db, i % 2 === 0 ? 'TRUNCATE' : 'RESTART'), true);
      t.db.exec(`UPDATE t SET name = 'upd-${i}' WHERE id = ${i + 1}`);
      fill(t, 10, `round-${i}`);
      await replica.sync();
    }

    const files = await db.l0Files();
    assert.deepEqual(files.map((f) => f.maxTXID), [1n, 2n, 3n, 4n, 5n, 6n]);
    for (const f of files.slice(1)) {
      const s = await summarize(db, f.maxTXID);
      assert.equal(s.walOffset, BigInt(WALHeaderSize));
      assert.ok(s.pages < snap.pages, `TXID ${f.maxTXID} is incremental (${s.pages} pages)`);
    }

    await assertRestoreMatches(replica, t);
  } finally {
    db.close();
    replica.close();
    t.close();
  }
});

test('checkpoint(RESTART) ignores frames of the old WAL left on disk', async () => {
  const t = createWALDB(['a']);
  fill(t, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    assert.equal(await replica.checkpoint(t.db, 'RESTART'), true);

    // The new WAL is much shorter than the old one, whose frames stay on disk.
    t.db.exec("INSERT INTO t (name) VALUES ('after')");
    const info = await replica.sync();
    assert.equal(info?.maxTXID, 2n);

    await assertRestoreMatches(replica, t);
  } finally {
    replica.close();
    t.close();
  }
});

test('a WAL started by another connection falls back to a snapshot', async () => {
  const t = createWALDB(['a', 'b']);
  fill(t, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const db = await LocalDatabase.open(t.path);
  const other = new DatabaseSync(t.path);
  try {
    await replica.sync();
    // The checkpoint runs on a connection that does not write: the writer,
    // which never reset the WAL, starts the next one with random salts.
    assert.equal(await replica.checkpoint(other, 'TRUNCATE'), true);
    t.db.exec("INSERT INTO t (name) VALUES ('after')");
    await replica.sync();

    const s = await summarize(db, 2n);
    assert.equal(s.pages, s.commit, 'the next file is a full snapshot');
    await assertRestoreMatches(replica, t);
  } finally {
    other.close();
    db.close();
    replica.close();
    t.close();
  }
});

for (const mode of ['RESTART', 'TRUNCATE']) {
  test(`a checkpoint run directly with PRAGMA wal_checkpoint(${mode}) still produces a snapshot`, async () => {
    const t = createWALDB(['a', 'b']);
    fill(t, 100, 'base');
    const client = new FileReplicaClient(makeTempDir('repl-'));
    const replica = new Replica(t.path, client);
    const db = await LocalDatabase.open(t.path);
    try {
      await replica.sync();
      t.db.prepare(`PRAGMA wal_checkpoint(${mode})`).get();
      // With RESTART, the frames of the old WAL stay on disk past the new one.
      t.db.exec("INSERT INTO t (name) VALUES ('after')");
      const info = await replica.sync();
      assert.equal(info?.maxTXID, 2n, 'the new WAL is not missed');

      const s = await summarize(db, 2n);
      assert.equal(s.pages, s.commit, 'the next file is a full snapshot');
      await assertRestoreMatches(replica, t);
    } finally {
      db.close();
      replica.close();
      t.close();
    }
  });
}

test('a commit between the sync and the checkpoint produces a snapshot', async () => {
  const t = createWALDB(['a', 'b']);
  fill(t, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const db = await LocalDatabase.open(t.path);
  try {
    await db.sync();
    // Not synced: the checkpoint would move it into the database file.
    t.db.exec("INSERT INTO t (name) VALUES ('unsynced')");
    assert.equal(await db.checkpoint(t.db, 'TRUNCATE'), false);

    t.db.exec("INSERT INTO t (name) VALUES ('after')");
    await db.sync();
    const s = await summarize(db, 2n);
    assert.equal(s.pages, s.commit, 'the next file is a full snapshot');

    await replica.sync();
    await assertRestoreMatches(replica, t);
  } finally {
    db.close();
    replica.close();
    t.close();
  }
});

test('a busy checkpoint leaves the next sync incremental in the old WAL', async () => {
  const t = createWALDB(['a', 'b']);
  fill(t, 100, 'base');
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  const db = await LocalDatabase.open(t.path);
  const reader = new DatabaseSync(t.path, { readOnly: true });
  try {
    await replica.sync();

    // A reader pinned on the WAL keeps RESTART from completing.
    reader.exec('BEGIN');
    reader.prepare('SELECT count(*) FROM t').get();
    t.db.exec('PRAGMA busy_timeout=0');
    assert.equal(await replica.checkpoint(t.db, 'TRUNCATE'), false);

    t.db.exec("INSERT INTO t (name) VALUES ('after')");
    await replica.sync();
    const s = await summarize(db, 2n);
    assert.ok(s.walOffset > BigInt(WALHeaderSize), 'resumes in the old WAL');
    assert.ok(s.pages < s.commit, 'the next file is incremental');

    reader.exec('COMMIT');
    await assertRestoreMatches(replica, t);
  } finally {
    reader.close();
    db.close();
    replica.close();
    t.close();
  }
});

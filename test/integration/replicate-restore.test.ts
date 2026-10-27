import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { ErrReplicaAhead, FileReplicaClient, Replica } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

test('end-to-end: replicate a WAL database to a file replica and restore', async () => {
  const t = createWALDB(['a', 'b', 'c']); // connection stays open (WAL retained)
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    // First sync produces TXID=1 (local write + upload).
    const info1 = await replica.sync();
    assert.equal(info1?.minTXID, 1n);
    assert.equal(info1?.maxTXID, 1n);

    const files1: bigint[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      files1.push(f.maxTXID);
    }
    assert.deepEqual(files1, [1n]);

    // Insert more rows and sync again -> TXID=2.
    t.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
    const info2 = await replica.sync();
    assert.equal(info2?.minTXID, 2n);
    assert.equal(info2?.maxTXID, 2n);

    const files2: bigint[] = [];
    for await (const f of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
      files2.push(f.maxTXID);
    }
    files2.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    assert.deepEqual(files2, [1n, 2n]);

    const status = await replica.syncStatus();
    assert.equal(status.inSync, true);
    assert.equal(status.localTXID, 2n);
    assert.equal(status.remoteTXID, 2n);

    // Restore into a fresh path and verify integrity + row count.
    const out = join(makeTempDir('restore-'), 'restored.db');
    await replica.restore({ outputPath: out });

    const restored = new DatabaseSync(out);
    const ic = restored.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
    assert.equal(ic.integrity_check, 'ok');
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 5);
    restored.close();
  } finally {
    replica.close();
    t.close();
  }
});

test('ensureExists restores a missing local database', async () => {
  const t = createWALDB(['x', 'y']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  try {
    await replica.sync();

    // Remove the local database file (+ wal/shm) so it is "missing".
    await rm(t.path, { force: true });
    await rm(t.walPath, { force: true });
    await rm(`${t.path}-shm`, { force: true });

    await replica.ensureExists();

    const restored = new DatabaseSync(t.path);
    const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
    assert.equal(count.n, 2);
    restored.close();
  } finally {
    replica.close();
    t.close();
  }
});

test('sync replicates a header-only (empty) database', async () => {
  const dir = makeTempDir('empty-');
  const path = join(dir, 'empty.db');

  // The library is driver-agnostic and never creates the database itself, so
  // create an (empty) SQLite DB here with the application's own driver.
  const conn = new DatabaseSync(path);
  conn.exec('PRAGMA journal_mode=WAL');
  conn.close();

  const client = new FileReplicaClient(join(dir, 'repl'));
  const replica = new Replica(path, client);
  try {
    // Even an empty SQLite DB has a header page (page_count >= 1).
    const info = await replica.sync();
    assert.equal(info?.minTXID, 1n);
    let n = 0;
    for await (const _ of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n++;
    assert.equal(n, 1);
  } finally {
    replica.close();
  }
});

function countRows(path: string): number {
  const conn = new DatabaseSync(path);
  try {
    return (conn.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
  } finally {
    conn.close();
  }
}

test('writes made after restoring a database on another host are replicated', async () => {
  const t = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const first = new Replica(t.path, client);

  // The new writer restores the database into its own path and keeps writing.
  const path = join(makeTempDir('second-'), 'second.db');
  const second = new Replica(path, client);

  try {
    await first.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await first.sync();

    await second.restore({ outputPath: path });
    assert.equal((await second.syncStatus()).localTXID, 2n);

    const conn = new DatabaseSync(path);
    conn.exec('PRAGMA journal_mode=WAL');
    conn.exec('PRAGMA wal_autocheckpoint=0');
    try {
      conn.exec("INSERT INTO t (name) VALUES ('d')");

      // The first sync after the restore continues the sequence, from a snapshot.
      const info = await second.sync();
      assert.equal(info?.minTXID, 3n);
      assert.deepEqual(await second.syncStatus(), { localTXID: 3n, remoteTXID: 3n, inSync: true });

      // Later syncs are incremental again.
      conn.exec("INSERT INTO t (name) VALUES ('e')");
      assert.equal((await second.sync())?.minTXID, 4n);
    } finally {
      conn.close();
    }

    const out = join(makeTempDir('restore-'), 'restored.db');
    await second.restore({ outputPath: out });
    assert.equal(countRows(out), 5);
  } finally {
    first.close();
    second.close();
    t.close();
  }
});

test('restore replaces an existing database, its WAL and its local cache', async () => {
  const t = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  // A stale copy of the database, with its own WAL and replication history.
  const path = join(makeTempDir('stale-'), 'stale.db');
  const stale = new DatabaseSync(path);
  stale.exec('PRAGMA journal_mode=WAL');
  stale.exec('PRAGMA wal_autocheckpoint=0');
  stale.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  stale.exec("INSERT INTO t (name) VALUES ('stale')");
  const staleReplica = new Replica(path, new FileReplicaClient(makeTempDir('stale-repl-')));
  await staleReplica.sync();
  staleReplica.close();

  const restored = new Replica(path, client);

  try {
    await replica.sync();

    // Copy the WAL aside, close the connection (which checkpoints and removes
    // the WAL), then put the stale WAL back, as after a crash.
    const wal = await readFile(`${path}-wal`);
    stale.close();
    await writeFile(`${path}-wal`, wal);

    await restored.restore({ outputPath: path });
    assert.equal(countRows(path), 3);
    assert.equal((await restored.syncStatus()).localTXID, 1n);
  } finally {
    restored.close();
    replica.close();
    t.close();
  }
});

test('sync fails when the replica is ahead of the local database', async () => {
  const t = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);

  // Another database replicated to the same replica, without restoring it.
  const other = createWALDB(['x']);
  const otherReplica = new Replica(other.path, client);

  try {
    await replica.sync();
    t.db.exec("INSERT INTO t (name) VALUES ('b')");
    await replica.sync();

    await assert.rejects(otherReplica.sync(), (err: unknown) => {
      assert.ok(err instanceof ErrReplicaAhead);
      assert.equal(err.code, 'ERR_REPLICA_AHEAD');
      assert.equal(err.localTXID, 1n);
      assert.equal(err.remoteTXID, 2n);
      return true;
    });
  } finally {
    otherReplica.close();
    replica.close();
    other.close();
    t.close();
  }
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';

import { FileReplicaClient, Replica, type ReaderConnection } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** Wraps a read-only node:sqlite connection, recording how it is used. */
function trackingReader(path: string, calls: string[], failReads = 0): ReaderConnection {
  const conn = new DatabaseSync(path, { readOnly: true });
  return {
    exec(sql) {
      calls.push(`exec:${sql}`);
      conn.exec(sql);
    },
    prepare(sql) {
      calls.push(`prepare:${sql}`);
      const stmt = conn.prepare(sql);
      return {
        get() {
          if (sql.includes('sqlite_master') && failReads > 0) {
            failReads--;
            throw new Error('simulated read failure');
          }
          return stmt.get();
        },
      };
    },
    close() {
      calls.push('close');
      conn.close();
    },
  };
}

test('openReader: the supplied connection pins sync snapshots', async (t) => {
  const tdb = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const opened: string[] = [];
  const calls: string[] = [];
  const replica = new Replica(tdb.path, client, undefined, {
    openReader: (path) => {
      opened.push(path);
      return trackingReader(path, calls);
    },
  });
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync();
  tdb.db.exec("INSERT INTO t (name) VALUES ('d'), ('e')");
  await replica.sync();
  await replica.snapshot();

  assert.deepEqual(opened, [tdb.path], 'reader is opened once and cached');
  assert.ok(calls.includes('exec:BEGIN'));
  assert.ok(calls.includes('exec:COMMIT'));

  const out = join(makeTempDir('restore-'), 'restored.db');
  await replica.restore({ outputPath: out });
  const restored = new DatabaseSync(out);
  const count = restored.prepare('SELECT count(*) AS n FROM t').get() as { n: number };
  assert.equal(count.n, 5);
  restored.close();

  replica.close();
  assert.equal(calls.at(-1), 'close', 'replica.close() closes the reader');
});

test('a failed pinning read does not leave the read transaction open', async (t) => {
  const tdb = createWALDB(['a', 'b', 'c']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const calls: string[] = [];
  const replica = new Replica(tdb.path, client, undefined, {
    openReader: (path) => trackingReader(path, calls, 1),
  });
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await assert.rejects(replica.sync(), /simulated read failure/);
  assert.ok(calls.includes('exec:ROLLBACK'));

  // The next sync must start a fresh transaction rather than fail with
  // "cannot start a transaction within a transaction".
  const info = await replica.sync();
  assert.equal(info?.minTXID, 1n);
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

import {
  CompactionLevels,
  ErrCompactionTooEarly,
  ErrNoCompaction,
  FileReplicaClient,
  Replica,
} from '../../src/index.ts';
import type { FileInfo } from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

async function countLevel(client: FileReplicaClient, level: number): Promise<FileInfo[]> {
  const out: FileInfo[] = [];
  for await (const f of client.ltxFiles({ level, seek: 0n, useMetadata: false })) out.push(f);
  return out;
}

test('compactDB respects the per-level time bucket', async (t) => {
  const tdb = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  // Use a very large bucket so two immediate calls land in the same bucket.
  replica.compactionLevels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 600_000 },
    { level: 2, intervalMs: 1_200_000 },
    { level: 3, intervalMs: 1_800_000 },
  ]);
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync();
  tdb.db.exec("INSERT INTO t (name) VALUES ('b')");
  await replica.sync();

  const info = await replica.compactDB(1, Date.now());
  assert.ok(info, 'first compaction should produce a file');

  // Same bucket -> refuse to re-compact.
  await assert.rejects(() => replica.compactDB(1, Date.now()), ErrCompactionTooEarly);
});

test('compactDB returns ErrNoCompaction when nothing is pending', async (t) => {
  const tdb = createWALDB(['a']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  replica.compactionLevels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 600_000 },
    { level: 2, intervalMs: 1_200_000 },
    { level: 3, intervalMs: 1_800_000 },
  ]);
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync();
  await replica.compactDB(1, Date.now()); // L0 -> L1

  // A future bucket passes the time check; the destination already covers the
  // source, so there is nothing to compact.
  await assert.rejects(() => replica.compactDB(1, Date.now() + 600_000), ErrNoCompaction);
});

test('compactSnapshot refuses to re-snapshot within the same bucket', async (t) => {
  const tdb = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync();
  await replica.snapshot();

  // Re-snapshotting immediately lands in the same bucket -> too early.
  await assert.rejects(() => replica.compactSnapshot(Date.now(), 300_000), ErrCompactionTooEarly);
});

test('snapshot retention is time-based and keeps the newest snapshot', async (t) => {
  const tdb = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync();
  await replica.snapshot(); // snapshot A
  tdb.db.exec("INSERT INTO t (name) VALUES ('c')");
  await replica.sync();
  await replica.snapshot(); // snapshot B (newer)

  // Drive the time-based retention from the files' own creation times so the
  // cutoff lands deterministically between A and B. This does not depend on
  // wall-clock or a fixed sleep, so it cannot flake when a loaded CI runner
  // delays the test between writes.
  const snapshots = [...(await countLevel(client, 9))].sort(
    (x, y) => x.createdAt - y.createdAt,
  );
  assert.equal(snapshots.length, 2);
  const [a, b] = snapshots;
  assert.ok(b.createdAt > a.createdAt, 'the newer snapshot has a later created time');

  const cutoff = (a.createdAt + b.createdAt) / 2;
  const result = await replica.enforceSnapshotRetention(b.createdAt, b.createdAt - cutoff, 0);
  assert.equal(result.deleted, 1, 'the older snapshot is aged out');
  const kept = await countLevel(client, 9);
  assert.equal(kept.length, 1, 'only the newest snapshot remains');
});

test('L0 time retention deletes compacted L0 once it ages out, keeping the head', async (t) => {
  const tdb = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  t.after(() => replica.close());
  t.after(() => tdb.close());

  await replica.sync(); // L0 (1..1)
  tdb.db.exec("INSERT INTO t (name) VALUES ('c')");
  await replica.sync(); // L0 (2..2)
  await replica.compact({ dstLevel: 1 }); // L0 covered by L1 (1..2)

  // Use the oldest file's own creation time to place the cutoff well past it,
  // so the retention result is deterministic and does not depend on wall-clock
  // or a fixed sleep (which can flake on a loaded runner).
  const l0 = [...(await countLevel(client, 0))].sort((x, y) => x.createdAt - y.createdAt);
  const oldest = l0[0];
  const deleted = await replica.enforceL0RetentionByTime(oldest.createdAt + 1000, 500);
  assert.equal(deleted, 1, 'the older, compacted L0 file is deleted');

  const remaining = await countLevel(client, 0);
  assert.equal(remaining.length, 1, 'the live head L0 is retained');
  assert.equal(remaining[0].maxTXID, 2n);
});

test('run() compacts per level using time-bucketed scheduling', async (t) => {
  const tdb = createWALDB(['a', 'b']);
  const client = new FileReplicaClient(makeTempDir('repl-'));
  const replica = new Replica(tdb.path, client);
  const ac = new AbortController();
  const outPath = join(makeTempDir('restore-'), 's.db');
  const levels = new CompactionLevels([
    { level: 0, intervalMs: 0 },
    { level: 1, intervalMs: 30 },
    { level: 2, intervalMs: 60 },
    { level: 3, intervalMs: 120 },
  ]);

  const runPromise = replica.run({ intervalMs: 10, compactionLevels: levels, signal: ac.signal });
  // Always stop the run loop, or a failed assertion leaves it (and the test
  // process) running forever.
  t.after(async () => {
    ac.abort();
    await runPromise;
  });
  t.after(() => replica.close());
  t.after(() => tdb.close());

  for (let i = 0; i < 5; i++) {
    tdb.db.exec("INSERT INTO t (name) VALUES ('x')");
    await sleep(30);
  }

  // Wait for all rows to be replicated, then for a higher-level file to appear.
  await waitForRestoredRows(replica, outPath, 7, 8000);
  const deadline = Date.now() + 8000;
  for (;;) {
    let anyCompacted = 0;
    for (let level = 1; level <= 3; level++) {
      anyCompacted += (await countLevel(client, level)).length;
    }
    if (anyCompacted >= 1) break;
    if (Date.now() > deadline) throw new Error('timed out waiting for compaction');
    await sleep(100);
  }

  ac.abort();
  await runPromise;

  let compacted = 0;
  for (let level = 1; level <= 3; level++) compacted += (await countLevel(client, level)).length;
  assert.ok(compacted >= 1, 'per-level scheduling produced a higher-level file');
});

async function waitForRestoredRows(
  replica: Replica,
  outPath: string,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = 'no restore attempted';
  for (;;) {
    // The run loop is live: before the first sync lands the replica is empty
    // (NoMatchingBackupError), and compaction/retention may change the file set
    // mid-restore. Treat any failure as "not there yet" and retry.
    try {
      await replica.restore({ outputPath: outPath });
      const d = new DatabaseSync(outPath);
      const n = (d.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
      const ic = (d.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
        .integrity_check;
      d.close();
      if (n === expected && ic === 'ok') return;
      last = `got ${n} rows, integrity_check=${ic}`;
    } catch (err) {
      last = `restore failed: ${(err as Error).message}`;
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${expected} rows (${last})`);
    await sleep(100);
  }
}

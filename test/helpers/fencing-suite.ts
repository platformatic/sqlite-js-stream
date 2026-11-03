/**
 * Shared behavior tests for backends that implement fenced writes
 * (`ReplicaClient.registerGeneration`). Each backend package runs it against
 * its own client:
 *
 *   fencingSuite('s3', () => ({ prefix, client }));
 *
 * It imports the library by its package name, so the backend's clients and the
 * `Replica` used here share one copy of the error classes.
 */

import { describe, test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  Replica,
  type FileInfo,
  type ReplicaClient,
  type WriteLTXFileOptions,
} from '@platformatic/sqlite-js-stream';

/** A replica client with the fenced-write capability. */
export interface FencedClient extends ReplicaClient {
  registerGeneration(generation: bigint): Promise<void>;
}

/** Storage under test. Every client made by one backend shares its storage. */
export interface FencingBackend {
  /** Returns a prefix that no other test uses. */
  prefix(): string;
  /** Creates a client on `prefix`, fenced on `generation` (null: unfenced). */
  client(prefix: string, generation: bigint | null): FencedClient;
}

export interface FencingSuiteOptions {
  /** Skips the suite (e.g. when the backend's service is not available). */
  skip?: boolean | string;
}

const superseded = { code: 'ERR_GENERATION_SUPERSEDED' };

function tempDir(name: string): string {
  return mkdtempSync(join(tmpdir(), `sqlite-js-stream-fencing-${name}-`));
}

/** A WAL-mode database with one row per name, each in its own transaction. */
function createDB(names: string[]): { path: string; db: DatabaseSync } {
  const path = join(tempDir('db'), 'test.db');
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  for (const name of names) insert(db, name);
  return { path, db };
}

function insert(db: DatabaseSync, name: string): void {
  db.prepare('INSERT INTO t (name) VALUES (?)').run(name);
}

/** Opens a database restored by a new writer, ready for writes. */
function openRestored(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA wal_autocheckpoint=0');
  return db;
}

function names(path: string): string[] {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    return (db.prepare('SELECT name FROM t ORDER BY id').all() as Array<{ name: string }>).map((r) => r.name);
  } finally {
    db.close();
  }
}

/** Restores the latest state of `prefix` with an unfenced client. */
async function restoreLatest(backend: FencingBackend, prefix: string): Promise<{ names: string[]; pos: bigint }> {
  const reader = new Replica(join(tempDir('reader'), 'reader.db'), backend.client(prefix, null));
  try {
    const out = join(tempDir('restore'), 'restored.db');
    await reader.restore({ outputPath: out });
    return { names: names(out), pos: await reader.calcPos() };
  } finally {
    reader.close();
  }
}

/** Lists every LTX file of a level through `client`. */
async function list(client: ReplicaClient, level: number): Promise<FileInfo[]> {
  const out: FileInfo[] = [];
  for await (const info of client.ltxFiles({ level, seek: 0n, useMetadata: false })) out.push(info);
  return out;
}

/**
 * A client of a writer of `generation` whose uploads were already sent when it
 * was superseded: each upload goes through a new client (so nothing it learned
 * from an earlier rejection stops it), and rejections are ignored.
 */
function inFlightClient(backend: FencingBackend, prefix: string, generation: bigint): ReplicaClient {
  const base = backend.client(prefix, generation);
  return {
    type: () => base.type(),
    init: () => base.init(),
    ltxFiles: (opts) => base.ltxFiles(opts),
    openLtxFile: (opts) => base.openLtxFile(opts),
    deleteLtxFiles: (infos) => base.deleteLtxFiles(infos),
    deleteAll: () => base.deleteAll(),
    setLogger: (logger) => base.setLogger(logger),
    async writeLtxFile(opts: WriteLTXFileOptions, body: AsyncIterable<Uint8Array | string>): Promise<FileInfo> {
      try {
        return await backend.client(prefix, generation).writeLtxFile(opts, body);
      } catch (err) {
        if ((err as { code?: string }).code !== superseded.code) throw err;
        return { ...opts, size: 0, createdAt: Date.now() };
      }
    },
  };
}

/** Registers the fenced-write behavior tests for one backend. */
export function fencingSuite(
  name: string,
  setup: (t: TestContext) => FencingBackend,
  options: FencingSuiteOptions = {},
): void {
  describe(`fenced writes (${name})`, { skip: options.skip }, () => {
    test('registerGeneration only raises the generation', async (t) => {
      const backend = setup(t);
      const prefix = backend.prefix();

      await backend.client(prefix, null).registerGeneration(2n);
      await assert.rejects(backend.client(prefix, null).registerGeneration(1n), superseded);
      // Registering the current generation again, or a higher one, succeeds.
      await backend.client(prefix, null).registerGeneration(2n);
      await backend.client(prefix, null).registerGeneration(3n);
      await assert.rejects(backend.client(prefix, null).registerGeneration(2n), superseded);
    });

    test('a superseded writer fails and stops; a restore returns the new writer\'s data', async (t) => {
      const backend = setup(t);
      const prefix = backend.prefix();

      const a = createDB(['a1', 'a2']);
      const clientA = backend.client(prefix, 1n);
      await clientA.registerGeneration(1n);
      const writerA = new Replica(a.path, clientA);

      const pathB = join(tempDir('b'), 'b.db');
      const clientB = backend.client(prefix, 2n);
      const writerB = new Replica(pathB, clientB);
      let dbB: DatabaseSync | null = null;
      t.after(() => {
        writerA.close();
        writerB.close();
        dbB?.close();
        a.db.close();
      });

      await writerA.sync(); // TXID 1

      // B takes over: register, restore, write.
      await clientB.registerGeneration(2n);
      await writerB.restore({ outputPath: pathB });
      dbB = openRestored(pathB);
      insert(dbB, 'b1');

      // A keeps writing: its next sync is rejected, and so is every later one.
      insert(a.db, 'a3');
      await assert.rejects(writerA.sync(), superseded);
      assert.equal(writerA.superseded?.code, superseded.code);
      insert(a.db, 'a4');
      await assert.rejects(writerA.sync(), superseded);
      await assert.rejects(writerA.run({ intervalMs: 10 }), superseded);

      // B's sync of the same TXID succeeds.
      const info = await writerB.sync();
      assert.equal(info?.maxTXID, 2n);

      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1', 'a2', 'b1'], pos: 2n });
    });

    test('uploads of a superseded writer that land after the takeover are never read', async (t) => {
      const backend = setup(t);
      const prefix = backend.prefix();

      const a = createDB(['a1']);
      const clientA = backend.client(prefix, 1n);
      await clientA.registerGeneration(1n);
      const first = new Replica(a.path, clientA);
      await first.sync(); // TXID 1
      first.close();

      const pathB = join(tempDir('b'), 'b.db');
      const clientB = backend.client(prefix, 2n);
      const writerB = new Replica(pathB, clientB);
      const writerA = new Replica(a.path, inFlightClient(backend, prefix, 1n));
      let dbB: DatabaseSync | null = null;
      t.after(() => {
        writerA.close();
        writerB.close();
        dbB?.close();
        a.db.close();
      });

      await clientB.registerGeneration(2n);
      await writerB.restore({ outputPath: pathB });

      // A's uploads of TXIDs 2..4 race with B: they reach the replica after B
      // registered, ahead of B's own writes.
      for (const n of ['a2', 'a3', 'a4']) {
        insert(a.db, n);
        await writerA.sync();
      }
      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1'], pos: 1n });

      dbB = openRestored(pathB);
      insert(dbB, 'b1');
      await writerB.sync(); // TXID 2
      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1', 'b1'], pos: 2n });

      insert(dbB, 'b2');
      await writerB.sync(); // TXID 3
      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1', 'b1', 'b2'], pos: 3n });

      // Only B's files are listed above TXID 1.
      const l0 = await list(backend.client(prefix, null), 0);
      assert.deepEqual(l0.map((f) => f.maxTXID), [1n, 2n, 3n]);
    });

    test('compaction and deletions of a superseded writer fail', async (t) => {
      const backend = setup(t);
      const prefix = backend.prefix();

      const a = createDB(['a1']);
      const clientA = backend.client(prefix, 1n);
      await clientA.registerGeneration(1n);
      const writerA = new Replica(a.path, clientA);
      t.after(() => {
        writerA.close();
        a.db.close();
      });
      await writerA.sync(); // TXID 1
      insert(a.db, 'a2');
      await writerA.sync(); // TXID 2

      await backend.client(prefix, 2n).registerGeneration(2n);

      const before = await list(backend.client(prefix, null), 0);
      assert.equal(before.length, 2);

      await assert.rejects(clientA.deleteLtxFiles([before[0]]), superseded);
      await assert.rejects(clientA.deleteAll(), superseded);
      await assert.rejects(writerA.enforceRetentionByTXID(0, 3n), superseded);
      // The compacted file may still land (S3), but it holds TXIDs 1..2, which
      // B read when it registered, so it is consistent with the replica.
      await assert.rejects(writerA.compact({ dstLevel: 1 }), superseded);

      assert.deepEqual(await list(backend.client(prefix, null), 0), before);
      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1', 'a2'], pos: 2n });
    });

    test('the current writer can compact and delete', async (t) => {
      const backend = setup(t);
      const prefix = backend.prefix();

      const a = createDB(['a1']);
      const client = backend.client(prefix, 1n);
      await client.registerGeneration(1n);
      const writer = new Replica(a.path, client);
      t.after(() => {
        writer.close();
        a.db.close();
      });
      await writer.sync();
      insert(a.db, 'a2');
      await writer.sync();

      const l1 = await writer.compact({ dstLevel: 1 });
      assert.equal(l1?.maxTXID, 2n);
      assert.equal(await writer.enforceRetentionByTXID(0, 2n), 1);
      assert.deepEqual(await restoreLatest(backend, prefix), { names: ['a1', 'a2'], pos: 2n });
    });
  });
}

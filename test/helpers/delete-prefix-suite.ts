/**
 * Shared behavior tests for the `deletePrefix` static method of the replica
 * clients (file, PostgreSQL, S3). Each backend runs it against its storage:
 *
 *   deletePrefixSuite('pg', (t) => ({ root, client, deletePrefix, ... }));
 *
 * Errors are matched on their `code`, so a backend may use its own copy of the
 * library's error classes.
 */

import { describe, test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';

import { marshalHeader, type Header } from '../../src/ltx/index.ts';
import type { ReplicaClient } from '../../src/replica-client.ts';

/** Storage under test. */
export interface DeletePrefixBackend {
  /** Returns a root prefix that no other test uses (replicas go under it). */
  root(): string;
  /** Creates a client on `prefix`, fenced on `generation` (null: unfenced). */
  client(prefix: string, generation: bigint | null): ReplicaClient;
  /** Calls the backend's `deletePrefix`, returning the deleted replica prefixes. */
  deletePrefix(prefix: string, generation?: bigint | null): Promise<string[]>;
  /** Whether the backend supports generations (`registerGeneration`). */
  fenced: boolean;
  /** Whether a superseded deletion is guaranteed to delete nothing. */
  atomic: boolean;
}

export interface DeletePrefixSuiteOptions {
  /** Skips the suite (e.g. when the backend's service is not available). */
  skip?: boolean | string;
}

const superseded = { code: 'ERR_GENERATION_SUPERSEDED' };

function ltxBody(txid: bigint): Uint8Array {
  const hdr: Header = {
    version: 3,
    flags: 0,
    pageSize: 4096,
    commit: 1,
    minTXID: txid,
    maxTXID: txid,
    timestamp: 1700000000000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
  const head = marshalHeader(hdr);
  const out = new Uint8Array(head.length + 16);
  out.set(head);
  return out;
}

async function * once(data: Uint8Array): AsyncIterable<Uint8Array> {
  yield data;
}

/** Writes one LTX file per TXID on level 0. */
async function write(client: ReplicaClient, ...txids: bigint[]): Promise<void> {
  for (const txid of txids) {
    await client.writeLtxFile({ level: 0, minTXID: txid, maxTXID: txid }, once(ltxBody(txid)));
  }
}

/** Lists the TXIDs of level 0. */
async function listed(client: ReplicaClient): Promise<bigint[]> {
  const out: bigint[] = [];
  for await (const info of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) out.push(info.maxTXID);
  return out;
}

/** Creates a replica at `prefix` with files 1..n, registering `generation` when given. */
async function seed(
  backend: DeletePrefixBackend,
  prefix: string,
  n: number,
  generation: bigint | null = null,
): Promise<ReplicaClient> {
  const client = backend.client(prefix, null);
  await client.init();
  if (generation !== null && backend.fenced) await client.registerGeneration!(generation);
  await write(client, ...Array.from({ length: n }, (_, i) => BigInt(i + 1)));
  return client;
}

export function deletePrefixSuite(
  name: string,
  setup: (t: TestContext) => DeletePrefixBackend,
  opts: DeletePrefixSuiteOptions = {},
): void {
  describe(`deletePrefix (${name})`, { skip: opts.skip }, () => {
    test('deletes the replica at the prefix and every replica under it', async (t) => {
      const backend = setup(t);
      const root = backend.root();
      const ns = `${root}/ns`;
      await seed(backend, ns, 1, 3n);
      await seed(backend, `${ns}/a`, 2, 5n);
      await seed(backend, `${ns}/b/c`, 3, 7n);
      // Siblings sharing the string prefix, and other namespaces, are kept.
      const sibling = await seed(backend, `${root}/nsx`, 2, 9n);
      const other = await seed(backend, `${root}/other/a`, 1, 11n);

      const deleted = await backend.deletePrefix(ns);
      assert.deepEqual(deleted, [ns, `${ns}/a`, `${ns}/b/c`]);

      for (const prefix of [ns, `${ns}/a`, `${ns}/b/c`]) {
        assert.deepEqual(await listed(backend.client(prefix, null)), [], prefix);
      }
      assert.deepEqual(await listed(sibling), [1n, 2n]);
      assert.deepEqual(await listed(other), [1n]);

      // Deleting again finds nothing.
      assert.deepEqual(await backend.deletePrefix(ns), []);
    });

    test('accepts a prefix with leading and trailing slashes', async (t) => {
      const backend = setup(t);
      const ns = `${backend.root()}/ns`;
      await seed(backend, `${ns}/a`, 1);
      assert.deepEqual(await backend.deletePrefix(`/${ns}/`), [`${ns}/a`]);
    });

    test('returns nothing for a prefix without replicas', async (t) => {
      const backend = setup(t);
      assert.deepEqual(await backend.deletePrefix(`${backend.root()}/missing`), []);
    });

    test('rejects an empty prefix', async (t) => {
      const backend = setup(t);
      await assert.rejects(backend.deletePrefix(''), /non-empty/);
    });

    test('deletes the generations, so a new writer can register a lower one', async (t) => {
      const backend = setup(t);
      if (!backend.fenced) {
        t.skip('the backend has no generations');
        return;
      }
      const ns = `${backend.root()}/ns`;
      const prefix = `${ns}/a`;
      await seed(backend, prefix, 2, 10n);

      // Before the deletion, generation 1 is superseded.
      await assert.rejects(backend.client(prefix, null).registerGeneration!(1n), superseded);

      await backend.deletePrefix(ns);

      const fresh = backend.client(prefix, null);
      await fresh.registerGeneration!(1n);
      await write(fresh, 1n);
      assert.deepEqual(await listed(fresh), [1n]);
    });

    test('a fenced deletion fails if a replica has a higher generation', async (t) => {
      const backend = setup(t);
      if (!backend.fenced) {
        t.skip('the backend has no generations');
        return;
      }
      const ns = `${backend.root()}/ns`;
      const a = await seed(backend, `${ns}/a`, 1, 4n);
      const b = await seed(backend, `${ns}/b`, 2, 8n);

      await assert.rejects(backend.deletePrefix(ns, 7n), (err: Error & { code?: string; generation?: bigint }) => {
        assert.equal(err.code, superseded.code);
        assert.equal(err.generation, 8n);
        return true;
      });
      if (backend.atomic) {
        assert.deepEqual(await listed(a), [1n]);
        assert.deepEqual(await listed(b), [1n, 2n]);
      }

      // A generation at least as high as every replica's deletes them.
      assert.deepEqual(await backend.deletePrefix(ns, 8n), [`${ns}/a`, `${ns}/b`]);
      assert.deepEqual(await listed(b), []);
    });

    test('a former writer stays fenced until the deletion', async (t) => {
      const backend = setup(t);
      if (!backend.fenced) {
        t.skip('the backend has no generations');
        return;
      }
      const ns = `${backend.root()}/ns`;
      const prefix = `${ns}/a`;
      const old = backend.client(prefix, null);
      await old.registerGeneration!(1n);
      await write(old, 1n);
      await backend.client(prefix, null).registerGeneration!(2n);

      await assert.rejects(write(old, 2n), superseded);
      assert.deepEqual(await backend.deletePrefix(ns), [prefix]);
      assert.deepEqual(await listed(backend.client(prefix, null)), []);
    });
  });
}

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { marshalHeader, type Header } from '@platformatic/sqlite-js-stream';

import { deletePrefixSuite } from '../../sqlite-js-stream/test/helpers/delete-prefix-suite.ts';

import { PgLeaser, PgReplicaClient } from '../src/index.ts';
import { connectForTest, pgUrl } from './helpers/db.ts';

const skip = process.env.PG_URL ? false : 'PG_URL is not set';
// Own tables, so this file never creates the tables of the other test files
// concurrently.
const tablePrefix = 'delprefix_ltx';

let seq = 0;
function root(): string {
  return `delete-prefix/${process.pid}-${Date.now().toString(36)}-${seq++}`;
}

deletePrefixSuite(
  'pg',
  (t) => {
    const sql = connectForTest(t);
    return {
      root,
      client: (prefix, generation) => new PgReplicaClient({ sql, prefix, generation, tablePrefix }),
      deletePrefix: (prefix, generation) => PgReplicaClient.deletePrefix({ sql, prefix, generation, tablePrefix }),
      fenced: true,
      atomic: true,
    };
  },
  { skip },
);

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
  return marshalHeader(hdr);
}

async function * once(data: Uint8Array): AsyncIterable<Uint8Array> {
  yield data;
}

async function count(client: PgReplicaClient): Promise<number> {
  let n = 0;
  for await (const _ of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) n++;
  return n;
}

test('pg deletePrefix with a url opens and closes its own connection', { skip }, async (t) => {
  const sql = connectForTest(t);
  const prefix = root();
  const client = new PgReplicaClient({ sql, prefix: `${prefix}/a`, tablePrefix });
  await client.registerGeneration(3n);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, once(ltxBody(1n)));

  assert.deepEqual(await PgReplicaClient.deletePrefix({ url: pgUrl(), prefix, tablePrefix }), [`${prefix}/a`]);
  assert.equal(await count(client), 0);
});

test('pg deletePrefix in a transaction commits and rolls back with it', { skip }, async (t) => {
  const sql = connectForTest(t);
  const prefix = root();
  const client = new PgReplicaClient({ sql, prefix: `${prefix}/a`, tablePrefix });
  await client.registerGeneration(3n);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, once(ltxBody(1n)));

  await assert.rejects(
    sql.begin(async (tx) => {
      assert.deepEqual(await PgReplicaClient.deletePrefix({ sql: tx, prefix, tablePrefix }), [`${prefix}/a`]);
      throw new Error('rollback');
    }),
    /rollback/,
  );
  assert.equal(await count(client), 1);

  // A superseded deletion rolls back its savepoint only.
  await sql.begin(async (tx) => {
    await assert.rejects(PgReplicaClient.deletePrefix({ sql: tx, prefix, tablePrefix, generation: 2n }), {
      code: 'ERR_GENERATION_SUPERSEDED',
    });
    assert.deepEqual(await PgReplicaClient.deletePrefix({ sql: tx, prefix, tablePrefix }), [`${prefix}/a`]);
  });
  assert.equal(await count(client), 0);
});

test('pg deletePrefix deletes the lease rows', { skip }, async (t) => {
  const sql = connectForTest(t);
  const prefix = root();
  const leaser = new PgLeaser({ sql, prefix: `${prefix}/a`, tablePrefix, owner: 'a' });
  await leaser.acquireLease();

  assert.deepEqual(await PgReplicaClient.deletePrefix({ sql, prefix, tablePrefix }), [`${prefix}/a`]);
  const [{ n }] = await sql`
    SELECT count(*)::int AS n FROM ${sql(`public.${tablePrefix}_leases`)} WHERE prefix = ${`${prefix}/a`}
  `;
  assert.equal(n, 0);
});

test('pg deletePrefix waits for a fenced upload in flight', { skip }, async (t) => {
  const sql = connectForTest(t);
  const prefix = root();
  const client = new PgReplicaClient({ sql, prefix: `${prefix}/a`, tablePrefix });
  await client.registerGeneration(1n);

  // The upload holds the generation row FOR SHARE until its body ends.
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let started!: () => void;
  const inFlight = new Promise<void>((resolve) => { started = resolve; });
  const upload = client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, (async function * () {
    started();
    await gate;
    yield ltxBody(1n);
  })());
  await inFlight;
  // Let the upload's transaction lock the row.
  await new Promise((resolve) => setTimeout(resolve, 100));

  let deleted = false;
  const deletion = PgReplicaClient.deletePrefix({ sql, prefix, tablePrefix }).then((r) => {
    deleted = true;
    return r;
  });
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(deleted, false);

  release();
  await upload;
  assert.deepEqual(await deletion, [`${prefix}/a`]);
  assert.equal(await count(client), 0);
});

test('pg deletePrefix returns nothing when the tables do not exist', { skip }, async (t) => {
  const sql = connectForTest(t);
  assert.deepEqual(await PgReplicaClient.deletePrefix({ sql, prefix: root(), tablePrefix: 'missing_ltx' }), []);
});

test('pg deletePrefix validates its options', async () => {
  await assert.rejects(PgReplicaClient.deletePrefix({ prefix: 'a' }), /url or sql/);
  await assert.rejects(PgReplicaClient.deletePrefix({ url: 'postgres://localhost/x', prefix: 'a', tablePrefix: 'x;y' }), /invalid identifier/);
});

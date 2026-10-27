import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import type { Sql } from 'postgres';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  ErrGenerationSuperseded,
  Replica,
  marshalHeader,
  newReplicaClientFromURL,
  type Header,
} from '@platformatic/sqlite-js-stream';

import {
  PgReplicaClient,
  type PgReplicaClientOptions,
} from '../src/index.ts';
import { connectForTest } from './helpers/db.ts';
import { createWALDB } from './helpers/wal-db.ts';

function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}

function makeLtxBody(size = 3): { body: Uint8Array; timestamp: number } {
  const timestamp = 1700000000000;
  const hdr: Header = {
    version: 3,
    flags: 0,
    pageSize: 4096,
    commit: 1,
    minTXID: 1n,
    maxTXID: 1n,
    timestamp: BigInt(timestamp),
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
  const tail = Uint8Array.from({ length: size }, (_, i) => i);
  const body = concatBytes([marshalHeader(hdr), tail]);
  return { body, timestamp };
}

function makeClient(
  t: TestContext,
  opts: Partial<PgReplicaClientOptions> & { prefix?: string } = {},
): PgReplicaClient {
  const sql = connectForTest(t);
  return new PgReplicaClient({ sql, createTables: true, ...opts });
}

/** Like makeClient but also returns the connection for direct queries. */
function makeClientAndSql(
  t: TestContext,
  opts: Partial<PgReplicaClientOptions> & { prefix?: string } = {},
): { client: PgReplicaClient; sql: Sql } {
  const sql = connectForTest(t);
  return { client: new PgReplicaClient({ sql, createTables: true, ...opts }), sql };
}

let seq = 0;
function makePrefix(): string {
  return `test/${Date.now().toString(36)}/${seq++}`;
}

async function* iter(data: Uint8Array): AsyncIterable<Uint8Array> {
  yield data;
}

/** Yields `data` in fixed-size slices. */
async function* iterChunks(data: Uint8Array, chunkSize: number): AsyncIterable<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) {
    yield data.subarray(i, i + chunkSize);
  }
}

async function collect(stream: AsyncIterable<Uint8Array | string>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const c of stream) {
    chunks.push(typeof c === 'string' ? new TextEncoder().encode(c) : Uint8Array.from(c));
  }
  return concatBytes(chunks);
}

test('type() reports pg', (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  assert.equal(client.type(), 'pg');
});

test('prefix normalization strips leading/trailing slashes', (t) => {
  const client = makeClient(t, { prefix: '/a/b/' });
  assert.equal(client.prefix, 'a/b');
});

test('init() creates the LTX tables once and is idempotent', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  await client.init();
  await client.init();
  assert.ok(client.type() === 'pg');
});

test('writeLtxFile stores a file row plus chunks with the header timestamp', async (t) => {
  const { client, sql } = makeClientAndSql(t, { prefix: makePrefix() });
  const { body, timestamp } = makeLtxBody();
  const info = await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));

  assert.equal(info.level, 0);
  assert.equal(info.minTXID, 1n);
  assert.equal(info.maxTXID, 1n);
  assert.equal(info.size, body.length);
  assert.equal(info.createdAt, timestamp);

  const fileRow = await sql`
    SELECT size, created_at FROM ${sql('public.ltx_files')}
    WHERE prefix = ${client.prefix} AND level = 0 AND min_txid = '1' AND max_txid = '1'
  `;
  assert.equal(fileRow.length, 1);
  assert.equal(Number(fileRow[0].size), body.length);
  assert.equal(new Date(fileRow[0].created_at).getTime(), timestamp);

  const chunkRow = await sql`
    SELECT count(*)::int AS n FROM ${sql('public.ltx_chunks')}
    WHERE prefix = ${client.prefix} AND level = 0 AND min_txid = '1' AND max_txid = '1'
  `;
  assert.equal(chunkRow[0].n, 1);
});

test('writeLtxFile is atomic under a duplicate key (writable only once)', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody();
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));
  await assert.rejects(
    () => client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body)),
    /write ltx file/,
  );
});

test('round-trips a file split across many chunks', async (t) => {
  const { body, timestamp } = makeLtxBody(1000);
  const client = makeClient(t, { prefix: makePrefix(), chunkSize: 64 });

  const info = await client.writeLtxFile(
    { level: 9, minTXID: 1n, maxTXID: 99n },
    iterChunks(body, 50),
  );
  assert.equal(info.size, body.length);
  assert.equal(info.createdAt, timestamp);

  const full = await collect(
    await client.openLtxFile({ level: 9, minTXID: 1n, maxTXID: 99n, offset: 0, size: 0 }),
  );
  assert.deepEqual([...full], [...body]);
});

test('ltxFiles lists files ordered and honors seek', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody();
  await client.writeLtxFile({ level: 0, minTXID: 5n, maxTXID: 6n }, iter(body));
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 4n }, iter(body));
  await client.writeLtxFile({ level: 0, minTXID: 2n, maxTXID: 2n }, iter(body));

  const infos = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    infos.push(i);
  }
  assert.deepEqual(infos.map((i) => i.minTXID), [1n, 2n, 5n]);
  assert.equal(infos[0].size, body.length);

  const after = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 3n, useMetadata: false })) {
    after.push(i.minTXID);
  }
  assert.deepEqual(after, [5n]);
});

test('openLtxFile reads a full file and byte ranges', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody(1000);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));

  const full = await collect(
    await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n, offset: 0, size: 0 }),
  );
  assert.deepEqual([...full], [...body]);

  const part = await collect(
    await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n, offset: 100, size: 60 }),
  );
  assert.deepEqual([...part], [...body.subarray(100, 160)]);

  const tail = await collect(
    await client.openLtxFile({
      level: 0, minTXID: 1n, maxTXID: 1n,
      offset: body.length - 10, size: 100,
    }),
  );
  assert.deepEqual([...tail], [...body.subarray(body.length - 10)]);
});

test('openLtxFile returns an empty stream when offset >= size', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody(5);
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));
  const buf = await collect(
    await client.openLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n, offset: 1000, size: 10 }),
  );
  assert.equal(buf.length, 0);
});

test('openLtxFile throws for a missing file', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  await assert.rejects(
    () => client.openLtxFile({ level: 0, minTXID: 99n, maxTXID: 99n, offset: 0, size: 0 }),
    /open ltx file/,
  );
});

test('deleteLtxFiles removes only the requested files', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody();
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));
  await client.writeLtxFile({ level: 0, minTXID: 2n, maxTXID: 2n }, iter(body));

  await client.deleteLtxFiles([
    { level: 0, minTXID: 1n, maxTXID: 1n, size: body.length, createdAt: 0 },
  ]);

  const remaining = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    remaining.push(i.minTXID);
  }
  assert.deepEqual(remaining, [2n]);
});

test('deleteAll removes every file for the prefix', async (t) => {
  const client = makeClient(t, { prefix: makePrefix() });
  const { body } = makeLtxBody();
  await client.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));
  await client.writeLtxFile({ level: 9, minTXID: 1n, maxTXID: 1n }, iter(body));

  await client.deleteAll();

  const infos = [];
  for await (const i of client.ltxFiles({ level: 0, seek: 0n, useMetadata: false })) {
    infos.push(i);
  }
  for await (const i of client.ltxFiles({ level: 9, seek: 0n, useMetadata: false })) {
    infos.push(i);
  }
  assert.equal(infos.length, 0);
});

test('newReplicaClientFromURL parses both postgres schemes', async () => {
  // Importing the addon registers the schemes; parse a full URL end-to-end.
  const rc = newReplicaClientFromURL('postgres://user:pass@localhost:5432/mydb?prefix=app/db1');
  assert.ok(rc instanceof PgReplicaClient);
  const pg = rc as PgReplicaClient;
  assert.equal(pg.prefix, 'app/db1');
  assert.equal(pg.type(), 'pg');
  await pg.close();

  const rc2 = newReplicaClientFromURL('postgresql://localhost/mydb?prefix=x');
  assert.ok(rc2 instanceof PgReplicaClient);
  assert.equal((rc2 as PgReplicaClient).prefix, 'x');
  await (rc2 as PgReplicaClient).close();
});

test('registerGeneration fences older writers (ErrGenerationSuperseded)', async (t) => {
  const writer = makeClient(t, { prefix: makePrefix(), generation: 1n });
  await writer.registerGeneration(1n);

  // A newer writer registers a higher generation.
  const newer = makeClient(t, { prefix: writer.prefix });
  await newer.registerGeneration(2n);

  const { body } = makeLtxBody();
  await assert.rejects(
    () => writer.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body)),
    ErrGenerationSuperseded,
  );

  // Registering a lower generation than the stored one is also rejected.
  await assert.rejects(() => newer.registerGeneration(1n), ErrGenerationSuperseded);

  // A writer at the current generation can write.
  const current = makeClient(t, { prefix: writer.prefix, generation: 2n });
  const info = await current.writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, iter(body));
  assert.equal(info.minTXID, 1n);
});

test('integrates with Replica.sync() then restore() via Postgres', async (t) => {
  const source = createWALDB(['a', 'b', 'c', 'd', 'e', 'f']);
  const client = makeClient(t, { prefix: makePrefix() });

  const replica = new Replica(source.path, client);
  try {
    await replica.sync();

    const outDir = mkdtempSync(join(tmpdir(), 'sqlite-js-stream-pg-restore-'));
    const outPath = join(outDir, 'restored.db');
    await replica.restore({ outputPath: outPath });

    const r = new DatabaseSync(outPath);
    const n = (r.prepare('SELECT count(*) AS n FROM t').get() as { n: number }).n;
    const integrity = (r.prepare('PRAGMA integrity_check').get() as { integrity_check: string })
      .integrity_check;
    r.close();
    assert.equal(n, 6);
    assert.equal(integrity, 'ok');
    rmSync(outDir, { recursive: true, force: true });
  } finally {
    replica.close();
    source.close();
  }
});

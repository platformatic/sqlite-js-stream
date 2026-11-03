import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  UploadPartCommand,
} from '@aws-sdk/client-s3';

import {
  ErrGenerationSuperseded,
  marshalHeader,
  type FileInfo,
  type Header,
} from '@platformatic/sqlite-js-stream';

import { fencingSuite } from '../../../test/helpers/fencing-suite.ts';

import { S3ReplicaClient, isFenced } from '../src/index.ts';
import { MemoryS3 } from './helpers/memory-s3.ts';

let seq = 0;

fencingSuite('s3', () => {
  const s3 = new MemoryS3();
  return {
    prefix: () => `db-${seq++}`,
    client: (prefix, generation) => new S3ReplicaClient({ bucket: 'b', prefix, client: s3, generation }),
  };
});

function makeLtxBody(tail = 3): Uint8Array {
  const hdr: Header = {
    version: 3,
    flags: 0,
    pageSize: 4096,
    commit: 1,
    minTXID: 1n,
    maxTXID: 1n,
    timestamp: 1700000000000n,
    preApplyChecksum: 0n,
    walOffset: 0n,
    walSize: 0n,
    walSalt1: 0,
    walSalt2: 0,
    nodeID: 0n,
  };
  const head = marshalHeader(hdr);
  const out = new Uint8Array(head.length + tail);
  out.set(head);
  for (let i = 0; i < tail; i++) out[head.length + i] = i & 0xff;
  return out;
}

async function* iter(data: Uint8Array, chunkSize: number): AsyncIterable<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) yield data.subarray(i, i + chunkSize);
}

function write(client: S3ReplicaClient, txid: bigint, body = makeLtxBody()): Promise<FileInfo> {
  return client.writeLtxFile({ level: 0, minTXID: txid, maxTXID: txid }, iter(body, 1024 * 1024));
}

async function listed(client: S3ReplicaClient, level = 0): Promise<bigint[]> {
  const out: bigint[] = [];
  for await (const info of client.ltxFiles({ level, seek: 0n, useMetadata: false })) out.push(info.maxTXID);
  return out;
}

function key(txid: bigint): string {
  const t = txid.toString(16).padStart(16, '0');
  return `b/db/ltx/0/${t}-${t}.ltx`;
}

function setup(): { s3: MemoryS3; client: (generation: bigint | null) => S3ReplicaClient } {
  const s3 = new MemoryS3();
  return { s3, client: (generation) => new S3ReplicaClient({ bucket: 'b', prefix: 'db', client: s3, generation }) };
}

function isLtxPut(command: unknown): boolean {
  return command instanceof PutObjectCommand && (command.input.Key ?? '').endsWith('.ltx');
}

test('isFenced: a file is ignored above the fence of a higher generation', () => {
  const fences = [
    { generation: 2n, txid: 10n },
    { generation: 5n, txid: 20n },
  ];
  assert.equal(isFenced(fences, 1n, 10n), true);
  assert.equal(isFenced(fences, 1n, 11n), false);
  assert.equal(isFenced(fences, 2n, 20n), true);
  assert.equal(isFenced(fences, 2n, 21n), false);
  assert.equal(isFenced(fences, 5n, 100n), true);
  assert.equal(isFenced([], 0n, 100n), true);
});

test('registerGeneration records a fence at the latest TXID', async () => {
  const { client } = setup();
  const unfenced = client(null);
  await write(unfenced, 1n);
  await write(unfenced, 2n);
  assert.equal(await unfenced.getGenerationState(), null);

  const writer = client(null);
  await writer.registerGeneration(5n);
  assert.equal(writer.getGeneration(), 5n);
  assert.deepEqual(await writer.getGenerationState(), {
    generation: 5n,
    fences: [{ generation: 5n, txid: 2n }],
  });

  // Registering it again keeps the fence.
  await write(writer, 3n);
  await client(null).registerGeneration(5n);
  assert.deepEqual((await writer.getGenerationState())?.fences, [{ generation: 5n, txid: 2n }]);

  // Unfenced uploads after the fence are not part of the replica.
  await write(unfenced, 4n);
  assert.deepEqual(await listed(writer), [1n, 2n, 3n]);
});

test('fenced uploads carry their generation and are written once', async () => {
  const { s3, client } = setup();
  const writer = client(3n);
  await writer.registerGeneration(3n);
  await write(writer, 1n);
  const stored = s3.objects.get(key(1n));
  assert.equal(stored?.metadata['ltx-generation'], '3');

  // A retried upload of the same file by the same generation succeeds and
  // leaves the object alone.
  const info = await write(client(3n), 1n, makeLtxBody(10));
  assert.equal(info.size, stored!.body.length);
  assert.equal(s3.objects.get(key(1n)), stored);

  // A lower generation cannot overwrite it.
  await assert.rejects(write(client(2n), 1n), ErrGenerationSuperseded);
  assert.equal(s3.objects.get(key(1n)), stored);
});

test('a superseded writer\'s file is replaced, and files ahead of the new writer are ignored', async () => {
  const { s3, client } = setup();
  await client(1n).registerGeneration(1n);
  await client(2n).registerGeneration(2n);

  // Uploads of generation 1 that were in flight land, but the writer is told.
  await assert.rejects(write(client(1n), 1n), ErrGenerationSuperseded);
  await assert.rejects(write(client(1n), 2n), ErrGenerationSuperseded);
  assert.ok(s3.objects.has(key(1n)));
  assert.ok(s3.objects.has(key(2n)));
  assert.deepEqual(await listed(client(null)), []);

  // A writer that saw the rejection stops before uploading anything else.
  const stale = client(1n);
  await assert.rejects(write(stale, 3n), ErrGenerationSuperseded);
  await assert.rejects(write(stale, 4n), ErrGenerationSuperseded);
  assert.ok(!s3.objects.has(key(4n)));

  await write(client(2n), 1n);
  assert.equal(s3.objects.get(key(1n))?.metadata['ltx-generation'], '2');
  assert.deepEqual(await listed(client(null)), [1n]);
});

test('a fenced multipart upload replaces a superseded writer\'s file', async () => {
  const { s3, client } = setup();
  await client(1n).registerGeneration(1n);
  await client(2n).registerGeneration(2n);
  await assert.rejects(write(client(1n), 1n), ErrGenerationSuperseded);

  const big = makeLtxBody(5 * 1024 * 1024 + 777);
  const info = await write(client(2n), 1n, big);
  assert.equal(info.size, big.length);
  assert.equal(s3.multiparts.size, 0);
  const stored = s3.objects.get(key(1n));
  assert.equal(stored?.metadata['ltx-generation'], '2');
  assert.equal(stored?.body.length, big.length);
  assert.deepEqual(await listed(client(null)), [1n]);

  // A retried multipart upload finds its own file and aborts its upload.
  await write(client(2n), 1n, big);
  assert.equal(s3.multiparts.size, 0);
  assert.equal(s3.objects.get(key(1n)), stored);
});

test('an upload that lands while a generation registers stays part of the replica', async () => {
  const { s3, client } = setup();
  await client(1n).registerGeneration(1n);
  await write(client(1n), 1n);

  // Generation 1 uploads TXID 2 after generation 2 was raised, but before the
  // new writer read the latest TXID.
  let raced = false;
  s3.onSend = async (command) => {
    if (raced || !(command instanceof ListObjectsV2Command)) return;
    const state = await client(null).getGenerationState();
    if (state?.generation !== 2n) return;
    raced = true;
    await assert.rejects(write(client(1n), 2n), ErrGenerationSuperseded);
  };
  await client(2n).registerGeneration(2n);
  assert.ok(raced);

  assert.deepEqual((await client(null).getGenerationState())?.fences, [
    { generation: 1n, txid: 0n },
    { generation: 2n, txid: 2n },
  ]);
  assert.deepEqual(await listed(client(null)), [1n, 2n]);
});

test('a generation registered between an upload and its check rejects the upload', async () => {
  const { s3, client } = setup();
  const writer = client(1n);
  await writer.registerGeneration(1n);

  // The new writer registers right after the upload lands.
  const send = s3.send.bind(s3);
  s3.send = async (command: any) => {
    const res = await send(command);
    if (isLtxPut(command)) {
      s3.send = send;
      await client(2n).registerGeneration(2n);
    }
    return res;
  };
  await assert.rejects(write(writer, 1n), ErrGenerationSuperseded);

  // The new writer read the file when it registered, so it is kept.
  assert.deepEqual((await client(null).getGenerationState())?.fences.at(-1), { generation: 2n, txid: 1n });
  assert.deepEqual(await listed(client(null)), [1n]);
});

test('listing makes no HeadObject request without fences, and caches generations', async () => {
  const { s3, client } = setup();
  let heads = 0;
  s3.onSend = (command) => {
    if (command instanceof HeadObjectCommand) heads++;
  };
  const unfenced = client(null);
  await write(unfenced, 1n);
  await write(unfenced, 2n);
  assert.deepEqual(await listed(unfenced), [1n, 2n]);
  assert.equal(heads, 0);

  const writer = client(1n);
  await writer.registerGeneration(1n); // fence at TXID 2
  await write(writer, 3n);
  await write(writer, 4n);

  // Files at or below the fence need no request; the writer knows its own.
  heads = 0;
  assert.deepEqual(await listed(writer), [1n, 2n, 3n, 4n]);
  assert.equal(heads, 0);

  // Another client asks once per file above the fence, then caches.
  const reader = client(null);
  assert.deepEqual(await listed(reader), [1n, 2n, 3n, 4n]);
  assert.equal(heads, 2);
  assert.deepEqual(await listed(reader), [1n, 2n, 3n, 4n]);
  assert.equal(heads, 2);
});

test('deleteAll keeps the generation object', async () => {
  const { s3, client } = setup();
  const writer = client(1n);
  await writer.registerGeneration(1n);
  await write(writer, 1n);
  await writer.deleteAll();
  assert.deepEqual([...s3.objects.keys()], ['b/db/generation.json']);
  await assert.rejects(client(null).registerGeneration(0n), ErrGenerationSuperseded);
});

test('MemoryS3 honors conditional requests like S3', async () => {
  const s3 = new MemoryS3();
  const put = (input: Record<string, unknown>) =>
    s3.send(new PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'v', ...input }));
  const precondition = { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } };

  const { ETag } = await put({ IfNoneMatch: '*' });
  await assert.rejects(put({ IfNoneMatch: '*' }), precondition);
  await assert.rejects(put({ IfMatch: '"other"' }), precondition);
  await assert.rejects(
    s3.send(new PutObjectCommand({ Bucket: 'b', Key: 'missing', Body: 'v', IfMatch: ETag })),
    precondition,
  );
  const { ETag: etag2 } = await s3.send(
    new PutObjectCommand({ Bucket: 'b', Key: 'k', Body: 'v2', IfMatch: ETag }),
  );
  assert.notEqual(etag2, ETag);

  // Reads.
  await assert.rejects(s3.send(new GetObjectCommand({ Bucket: 'b', Key: 'k', IfMatch: ETag })), precondition);
  await assert.rejects(s3.send(new GetObjectCommand({ Bucket: 'b', Key: 'k', IfNoneMatch: etag2 })), {
    $metadata: { httpStatusCode: 304 },
  });
  const head = await s3.send(new HeadObjectCommand({ Bucket: 'b', Key: 'k', IfMatch: etag2 }));
  assert.equal(head.ETag, etag2);
  const list = await s3.send(new ListObjectsV2Command({ Bucket: 'b' }));
  assert.equal(list.Contents[0].ETag, etag2);

  // A multipart completion that fails its precondition leaves the upload open.
  const { UploadId } = await s3.send(new CreateMultipartUploadCommand({ Bucket: 'b', Key: 'k' }));
  await s3.send(new UploadPartCommand({ Bucket: 'b', Key: 'k', UploadId, PartNumber: 1, Body: 'x' }));
  const complete = (input: Record<string, unknown>) =>
    s3.send(new CompleteMultipartUploadCommand({ Bucket: 'b', Key: 'k', UploadId, ...input }));
  await assert.rejects(complete({ IfNoneMatch: '*' }), precondition);
  assert.equal(s3.multiparts.size, 1);
  const done = await complete({ IfMatch: etag2 });
  assert.match(done.ETag, /-1"$/);
  assert.equal(s3.multiparts.size, 0);
});

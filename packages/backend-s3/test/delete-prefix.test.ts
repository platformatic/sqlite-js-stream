import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { CreateBucketCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

import { marshalHeader } from '@platformatic/sqlite-js-stream';

import { deletePrefixSuite, type DeletePrefixBackend } from '../../sqlite-js-stream/test/helpers/delete-prefix-suite.ts';

import { S3Leaser, S3ReplicaClient, type S3ClientLike } from '../src/index.ts';
import { MemoryS3 } from './helpers/memory-s3.ts';

// Runs against a real S3-compatible store when S3_ENDPOINT is set (with the
// credentials in the standard AWS environment variables), as CI does.
const endpoint = process.env.S3_ENDPOINT;
const bucket = process.env.S3_BUCKET ?? 'sqlite-js-stream-tests';
const skip = endpoint ? false : 'S3_ENDPOINT is not set';

let seq = 0;
function root(): string {
  return `delete-prefix/${process.pid}-${Date.now().toString(36)}-${seq++}`;
}

function newRealClient(): S3Client {
  return new S3Client({ endpoint, forcePathStyle: true, region: process.env.AWS_REGION ?? 'us-east-1' });
}

/** A client of the real store, destroyed after the test. */
function realClient(t: TestContext): S3Client {
  const client = newRealClient();
  t.after(() => client.destroy());
  return client;
}

if (endpoint) {
  const client = newRealClient();
  await client.send(new CreateBucketCommand({ Bucket: bucket })).catch((err) => {
    if (err?.name !== 'BucketAlreadyOwnedByYou' && err?.name !== 'BucketAlreadyExists') throw err;
  }).finally(() => client.destroy());
}

function backend(client: S3ClientLike, bucketName: string): DeletePrefixBackend {
  return {
    root,
    client: (prefix, generation) => new S3ReplicaClient({ bucket: bucketName, prefix, client, generation }),
    deletePrefix: (prefix, generation) => S3ReplicaClient.deletePrefix({ bucket: bucketName, prefix, client, generation }),
    fenced: true,
    // A superseded deletion stops before its first DeleteObjects request when
    // the replicas hold fewer than 1000 objects, as in the suite.
    atomic: true,
  };
}

deletePrefixSuite('s3 (in memory)', () => backend(new MemoryS3(), 'b'));

deletePrefixSuite('s3', (t) => backend(realClient(t), bucket), { skip });

test('s3 deletePrefix deletes every object under the prefix, across listing pages', { skip }, async (t) => {
  const client = realClient(t);
  const prefix = root();

  // More objects than a listing page or a DeleteObjects request holds.
  const keys: string[] = [`${prefix}/a/generation.json`, `${prefix}/a/lock.json`, `${prefix}/stray.txt`];
  for (let i = 0; i < 1100; i++) keys.push(`${prefix}/a/ltx/0/${i.toString(16).padStart(16, '0')}-${i.toString(16).padStart(16, '0')}.ltx`);
  keys.push(`${prefix}/b/c/ltx/1/0000000000000001-0000000000000001.ltx`);
  keys.push(`${prefix}x/ltx/0/0000000000000001-0000000000000001.ltx`);
  for (let i = 0; i < keys.length; i += 50) {
    await Promise.all(keys.slice(i, i + 50).map((Key) => client.send(new PutObjectCommand({
      Bucket: bucket,
      Key,
      Body: Key.endsWith('generation.json') ? '{"generation":"1","fences":[]}' : 'x',
    }))));
  }

  assert.deepEqual(await S3ReplicaClient.deletePrefix({ bucket, prefix, client }), [`${prefix}/a`, `${prefix}/b/c`]);

  const left = await client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
  assert.deepEqual((left.Contents ?? []).map((o) => o.Key), [`${prefix}x/ltx/0/0000000000000001-0000000000000001.ltx`]);
});

test('s3 deletePrefix deletes the lease objects', { skip }, async (t) => {
  const client = realClient(t);
  const prefix = root();
  await new S3Leaser({ bucket, prefix: `${prefix}/a`, client }).acquireLease();

  assert.deepEqual(await S3ReplicaClient.deletePrefix({ bucket, prefix, client }), [`${prefix}/a`]);
  const left = await client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: `${prefix}/` }));
  assert.equal(left.KeyCount ?? 0, 0);
});

test('s3 deletePrefix deletes the generation objects last', async () => {
  const s3 = new MemoryS3();
  const a = new S3ReplicaClient({ bucket: 'b', prefix: 'ns/a', client: s3 });
  await a.registerGeneration(2n);
  const deletes: string[][] = [];
  s3.onSend = (command) => {
    if (command.input?.Delete) deletes.push(command.input.Delete.Objects.map((o: { Key: string }) => o.Key));
  };
  await new S3ReplicaClient({ bucket: 'b', prefix: 'ns/a', client: s3 })
    .writeLtxFile({ level: 0, minTXID: 1n, maxTXID: 1n }, (async function * () {
      yield marshalHeader({
        version: 3, flags: 0, pageSize: 4096, commit: 1, minTXID: 1n, maxTXID: 1n, timestamp: 0n,
        preApplyChecksum: 0n, walOffset: 0n, walSize: 0n, walSalt1: 0, walSalt2: 0, nodeID: 0n,
      });
    })());

  assert.deepEqual(await S3ReplicaClient.deletePrefix({ bucket: 'b', prefix: 'ns', client: s3 }), ['ns/a']);
  assert.deepEqual(deletes, [['ns/a/ltx/0/0000000000000001-0000000000000001.ltx'], ['ns/a/generation.json']]);
});

test('s3 deletePrefix validates its options', async () => {
  const client = new MemoryS3();
  await assert.rejects(S3ReplicaClient.deletePrefix({ bucket: '', prefix: 'a', client }), /bucket is required/);
  await assert.rejects(S3ReplicaClient.deletePrefix({ bucket: 'b', prefix: '/', client }), /non-empty/);
});

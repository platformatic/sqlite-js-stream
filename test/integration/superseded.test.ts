import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  ErrGenerationSuperseded,
  FileReplicaClient,
  Replica,
  type FileInfo,
  type WriteLTXFileOptions,
} from '../../src/index.ts';
import { createWALDB, makeTempDir } from '../helpers/make-db.ts';

/** A file client that rejects uploads once `supersede()` is called. */
class FencedFileClient extends FileReplicaClient {
  writes = 0;
  private by: bigint | null = null;

  supersede(generation: bigint): void {
    this.by = generation;
  }

  override async writeLtxFile(
    opts: WriteLTXFileOptions,
    body: AsyncIterable<Uint8Array | string>,
  ): Promise<FileInfo> {
    this.writes++;
    if (this.by !== null) throw new ErrGenerationSuperseded(this.by);
    return super.writeLtxFile(opts, body);
  }
}

test('a superseded replica stops writing and run() rejects', async () => {
  const t = createWALDB(['a']);
  const client = new FencedFileClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    await replica.sync();
    const before = replica.superseded;
    assert.equal(before, null);

    client.supersede(7n);
    t.db.exec("INSERT INTO t (name) VALUES ('b')");
    await assert.rejects(replica.sync(), ErrGenerationSuperseded);
    assert.equal(replica.superseded?.generation, 7n);

    // Later operations fail without reaching the client.
    const writes = client.writes;
    t.db.exec("INSERT INTO t (name) VALUES ('c')");
    await assert.rejects(replica.sync(), ErrGenerationSuperseded);
    await assert.rejects(replica.snapshot(), ErrGenerationSuperseded);
    await assert.rejects(replica.compact(), ErrGenerationSuperseded);
    await assert.rejects(replica.enforceRetention(), ErrGenerationSuperseded);
    assert.equal(client.writes, writes);

    // run() stops instead of retrying, even without an abort signal.
    await assert.rejects(replica.run({ intervalMs: 10 }), ErrGenerationSuperseded);
  } finally {
    replica.close();
    t.close();
  }
});

test('run() stops when a sync is superseded', async () => {
  const t = createWALDB(['a']);
  const client = new FencedFileClient(makeTempDir('repl-'));
  const replica = new Replica(t.path, client);
  try {
    client.supersede(2n);
    await assert.rejects(replica.run({ intervalMs: 10 }), { code: 'ERR_GENERATION_SUPERSEDED' });
    assert.equal(client.writes, 1);
  } finally {
    replica.close();
    t.close();
  }
});

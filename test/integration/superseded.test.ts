import { test, type TestContext } from 'node:test';
import { rmSync } from 'node:fs';
import { dirname } from 'node:path';
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

/**
 * A database and a replica on a fenced file client. `after` hooks run in the
 * order they are registered, so the temporary directories are removed after
 * the replica and the database are closed.
 */
function setup(t: TestContext): { db: ReturnType<typeof createWALDB>; client: FencedFileClient; replica: Replica } {
  const db = createWALDB(['a']);
  t.after(() => db.close());
  const replicaDir = makeTempDir('repl-');
  const client = new FencedFileClient(replicaDir);
  const replica = new Replica(db.path, client);
  t.after(() => replica.close());
  t.after(() => {
    rmSync(dirname(db.path), { recursive: true, force: true });
    rmSync(replicaDir, { recursive: true, force: true });
  });
  return { db, client, replica };
}

test('a superseded replica stops writing and run() rejects', async (t) => {
  const { db, client, replica } = setup(t);

  await replica.sync();
  const before = replica.superseded;
  assert.equal(before, null);

  client.supersede(7n);
  db.db.exec("INSERT INTO t (name) VALUES ('b')");
  await assert.rejects(replica.sync(), ErrGenerationSuperseded);
  assert.equal(replica.superseded?.generation, 7n);

  // Later operations fail without reaching the client.
  const writes = client.writes;
  db.db.exec("INSERT INTO t (name) VALUES ('c')");
  await assert.rejects(replica.sync(), ErrGenerationSuperseded);
  await assert.rejects(replica.snapshot(), ErrGenerationSuperseded);
  await assert.rejects(replica.compact(), ErrGenerationSuperseded);
  await assert.rejects(replica.enforceRetention(), ErrGenerationSuperseded);
  assert.equal(client.writes, writes);

  // run() stops instead of retrying, even without an abort signal.
  await assert.rejects(replica.run({ intervalMs: 10 }), ErrGenerationSuperseded);
});

test('run() stops when a sync is superseded', async (t) => {
  const { client, replica } = setup(t);

  client.supersede(2n);
  await assert.rejects(replica.run({ intervalMs: 10 }), { code: 'ERR_GENERATION_SUPERSEDED' });
  assert.equal(client.writes, 1);
});

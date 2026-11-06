/**
 * Test worker: a replication thread that owns a Replica and answers
 * `sync` / `prepareCheckpoint` requests, like an application that keeps
 * replication off the thread that writes the database.
 */

import { parentPort } from 'node:worker_threads';

import { FileReplicaClient, Replica } from '../../src/index.ts';

let replica: Replica | null = null;

parentPort!.on('message', async (m: { id: number; type: string; dbPath?: string; clientPath?: string }) => {
  try {
    let result: unknown = null;
    if (m.type === 'open') {
      replica = new Replica(m.dbPath!, new FileReplicaClient(m.clientPath!));
    } else if (m.type === 'sync') {
      const info = await replica!.sync();
      result = info === null ? null : { level: info.level, maxTXID: String(info.maxTXID) };
    } else if (m.type === 'prepareCheckpoint') {
      result = await replica!.prepareCheckpoint();
    } else if (m.type === 'close') {
      replica!.close();
    }
    parentPort!.postMessage({ id: m.id, result });
  } catch (err) {
    parentPort!.postMessage({ id: m.id, error: String(err) });
  }
});

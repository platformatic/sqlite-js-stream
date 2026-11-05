// Answers queries on the restored database. It runs in a worker thread, as a
// read of a missing page blocks the thread until the main thread fills it.

import { DatabaseSync } from 'node:sqlite';
import { parentPort, workerData } from 'node:worker_threads';

const db = new DatabaseSync(workerData.uri);
const findUser = db.prepare('SELECT id, name, email FROM users WHERE id = ?');

parentPort?.on('message', ({ id, userId }: { id: number; userId: number }) => {
  try {
    parentPort?.postMessage({ id, user: findUser.get(userId) });
  } catch (err) {
    parentPort?.postMessage({ id, error: (err as Error).message });
  }
});

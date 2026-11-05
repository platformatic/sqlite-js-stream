// Worker which opens a database, runs statements, and posts the rows of a query
// or the error. Reads of a registered database must happen outside of the
// thread which fills it.

import { DatabaseSync } from 'node:sqlite';
import { parentPort, workerData } from 'node:worker_threads';

const { uri, exec = [], query } = workerData as { uri: string; exec?: string[]; query?: string };

try {
  const db = new DatabaseSync(uri);

  try {
    for (const sql of exec) {
      db.exec(sql);
    }

    const rows = query ? db.prepare(query).all().map(row => ({ ...row })) : [];
    parentPort?.postMessage({ rows });
  } finally {
    db.close();
  }
} catch (err) {
  const { message, errcode } = err as { message: string; errcode?: number };
  parentPort?.postMessage({ error: { message, errcode } });
}

/**
 * Test helper: runs SQL on a database opened with `workerData.uri`, in a
 * worker thread. A paged restore serves the pages its reads wait for from the
 * thread which started it, so its database is queried from another thread.
 *
 * Messages: `{ id, sql, params }` replies `{ id, rows }`; `{ id, exec }` runs
 * statements and replies `{ id }`; `{ id, close: true }` closes the database.
 * Failures reply `{ id, error }`.
 */

import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import { parentPort, workerData } from 'node:worker_threads';

const db = new DatabaseSync(workerData.uri as string);
db.exec('PRAGMA busy_timeout = 5000');
if (workerData.writer) db.exec('PRAGMA wal_autocheckpoint = 0');

interface Message {
  id: number;
  sql?: string;
  params?: SQLInputValue[];
  exec?: string;
  close?: boolean;
}

parentPort!.on('message', ({ id, sql, params = [], exec, close }: Message) => {
  try {
    if (close) {
      db.close();
      parentPort!.postMessage({ id });
    } else if (exec !== undefined) {
      db.exec(exec);
      parentPort!.postMessage({ id });
    } else {
      parentPort!.postMessage({ id, rows: db.prepare(sql!).all(...params) });
    }
  } catch (err) {
    parentPort!.postMessage({ id, error: (err as Error).message });
  }
});

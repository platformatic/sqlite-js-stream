// Restores source.db to restored.db page by page, while worker threads query
// it. source.db stands in for a replica in object storage: each read of it
// waits 20 ms, like a ranged GET would.

import { open, rm } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as sleep } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

import { databaseUri, register } from '../src/index.ts';

const SOURCE = 'source.db';
const TARGET = 'restored.db';

const replica = await open(SOURCE);

/** Reads a range of the replica. With object storage, this is a GET with a Range header. */
async function read(offset: number, length: number): Promise<Buffer> {
  await sleep(20);
  const buffer = Buffer.alloc(length);
  await replica.read(buffer, 0, length, offset);
  return buffer;
}

// The header of a database holds its page size and its page count
const header = await read(0, 100);
const pageSize = header.readUInt16BE(16) === 1 ? 65536 : header.readUInt16BE(16);
const pageCount = header.readUInt32BE(28);

// SQLite would apply the journals of a previous database to the new one
for (const suffix of ['', '-wal', '-shm', '-journal']) {
  await rm(TARGET + suffix, { force: true });
}

// The VFS calls this on the main thread when a worker waits for a missing page
let requested = 0;
const db = register({ path: TARGET, pageSize, pageCount }, pgno => {
  requested++;
  read((pgno - 1) * pageSize, pageSize).then(
    page => db.fill(pgno, page),
    () => db.fail(pgno), // Fails the read; the next read requests the page again
  );
});

// Meanwhile, download every page in order, 1 MiB at a time
async function download(): Promise<void> {
  const batch = (1024 * 1024) / pageSize;

  for (let first = 1; first <= pageCount; first += batch) {
    const count = Math.min(batch, pageCount - first + 1);
    const pages = await read((first - 1) * pageSize, count * pageSize);

    for (let i = 0; i < count; i++) {
      // Does nothing if the page is already present
      db.fill(first + i, pages.subarray(i * pageSize, (i + 1) * pageSize));
    }
  }
}

const start = performance.now();
const downloaded = download().then(() => performance.now() - start);

// The workers open the database through the VFS, and query it right away
const workers = Array.from({ length: 4 }, () => new Worker(new URL('./worker.ts', import.meta.url), {
  workerData: { uri: databaseUri(TARGET) },
}));

const pending = new Map<number, PromiseWithResolvers<unknown>>();
let nextId = 0;

for (const worker of workers) {
  worker.on('message', ({ id, user, error }: { id: number; user?: unknown; error?: string }) => {
    const request = pending.get(id)!;
    pending.delete(id);

    if (error) {
      request.reject(new Error(error));
    } else {
      request.resolve(user);
    }
  });
}

function findUser(userId: number): Promise<unknown> {
  const id = nextId++;
  const request = Promise.withResolvers<unknown>();
  pending.set(id, request);
  workers[id % workers.length].postMessage({ id, userId });
  return request.promise;
}

const users = await Promise.all([1, 123_456, 250_000, 499_999].map(findUser));
const answered = performance.now() - start;

console.log(users);
console.log(`Answered in ${answered.toFixed(0)} ms, with ${requested} pages requested on demand`);
console.log(`Downloaded all ${pageCount} pages in ${(await downloaded).toFixed(0)} ms`);

await Promise.all(workers.map(worker => worker.terminate()));
await replica.close();

// Once complete, the database is unregistered: it is a regular database, which
// any thread can open, with or without the VFS
console.log(`Complete: ${db.complete}`);

const local = new DatabaseSync(TARGET);
const { integrity_check: integrity } = local.prepare('PRAGMA integrity_check').get() as { integrity_check: string };
console.log(`Integrity check: ${integrity}`);
local.close();

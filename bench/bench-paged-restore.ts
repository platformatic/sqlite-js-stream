/**
 * Time to first response of a paged restore, against a full restore, as the
 * database grows.
 *
 * For each size, a database of incompressible rows (about one per page) is
 * replicated to a file replica, which is then read through a simulated
 * network: each request waits LATENCY_MS, and the bytes share one link of
 * BANDWIDTH_MBPS, as reads from S3 would. Then the database is restored:
 *
 *   - full:  restore() of the replica's own path, until it resolves, after
 *            which a query can run;
 *   - paged: restorePaged(), until it resolves (open), until a query from a
 *            worker thread returns a random row (first response), and until
 *            every page is local (filled).
 *
 * Tunables (env):
 *   SIZES           database sizes, in MiB                  (default 16,64,256,1024)
 *   LATENCY_MS      latency of each request, in ms          (default 20)
 *   BANDWIDTH_MBPS  bandwidth of the link, in MB/s          (default 100)
 *
 * Run (the paged VFS must be built, as pnpm test:paged-vfs does):
 *   pnpm bench:paged
 *   SIZES=16,64 LATENCY_MS=5 pnpm bench:paged
 */

import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';

import { databaseUri } from '@platformatic/sqlite-js-stream-paged-vfs';

import {
  FileReplicaClient,
  Replica,
  type LTXFilesOptions,
  type OpenLTXFileOptions,
  type ReplicaClient,
} from '../src/index.ts';

const SIZES = (process.env.SIZES ?? '16,64,256,1024').split(',').map(Number);
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 20);
const BANDWIDTH_MBPS = Number(process.env.BANDWIDTH_MBPS ?? 100);

/**
 * Reads `client` through a simulated network: every request (a read or a
 * listing) waits `latencyMs`, and the bytes read share one link of
 * `megabytesPerSecond`, whatever the number of concurrent reads.
 */
function network(client: ReplicaClient, latencyMs: number, megabytesPerSecond: number): ReplicaClient {
  const bytesPerMs = (megabytesPerSecond * 1e6) / 1000;
  let linkFree = 0;
  const transfer = async (bytes: number): Promise<void> => {
    const now = performance.now();
    linkFree = Math.max(now, linkFree) + bytes / bytesPerMs;
    if (linkFree - now >= 1) await sleep(linkFree - now);
  };

  return new Proxy(client, {
    get(target, prop) {
      if (prop === 'openLtxFile') {
        return async (opts: OpenLTXFileOptions) => {
          await sleep(latencyMs);
          const stream = await target.openLtxFile(opts);
          return Readable.from(
            (async function* () {
              for await (const chunk of stream) {
                await transfer(chunk.length);
                yield chunk;
              }
            })(),
          );
        };
      }
      if (prop === 'ltxFiles') {
        return (opts: LTXFilesOptions) =>
          (async function* () {
            await sleep(latencyMs);
            yield* target.ltxFiles(opts);
          })();
      }
      const value = Reflect.get(target, prop, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

/** Replicates a database of about `mib` MiB, then changes a few rows twice. Returns its number of rows. */
async function replicate(dir: string, client: ReplicaClient, mib: number): Promise<number> {
  const path = join(dir, 'source.db');
  const rows = mib * 256; // one row of 3,000 random bytes per 4 KiB page
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=OFF');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)');
  db.exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ${rows}) INSERT INTO t SELECT i, randomblob(3000) FROM n`);
  // The first sync reads the database file rather than a WAL of the same size
  db.exec('PRAGMA wal_checkpoint(TRUNCATE)');

  const replica = new Replica(path, client);
  await replica.sync();
  for (let i = 0; i < 2; i++) {
    db.exec(`UPDATE t SET v = randomblob(3000) WHERE id % 1000 = ${i}`);
    await replica.sync();
  }
  replica.close();
  db.close();
  return rows;
}

/** A worker thread which opens the database and reads one row when told, as a Durable Object host would. */
function queryWorker(): { ready: Promise<void>; query: (uri: string, id: number) => Promise<void>; close: () => Promise<number> } {
  const worker = new Worker(
    `
      const { parentPort } = require('node:worker_threads')
      const { DatabaseSync } = require('node:sqlite')
      parentPort.on('message', ({ uri, id }) => {
        const db = new DatabaseSync(uri)
        const row = db.prepare('SELECT length(v) AS n FROM t WHERE id = ?').get(id)
        db.close()
        parentPort.postMessage(row)
      })
    `,
    { eval: true },
  );
  return {
    ready: new Promise((resolve) => worker.once('online', () => resolve())),
    query: (uri, id) =>
      new Promise((resolve, reject) => {
        worker.once('message', (row: { n: number } | undefined) => (row?.n === 3000 ? resolve() : reject(new Error(`unexpected row ${JSON.stringify(row)}`))));
        worker.postMessage({ uri, id });
      }),
    close: () => worker.terminate(),
  };
}

function seconds(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`;
}

console.log(`Paged restore benchmark: Node ${process.version}, ${LATENCY_MS} ms per request, ${BANDWIDTH_MBPS} MB/s`);
console.log('');
console.log('| Database | Full restore | Paged: open | Paged: first response | Paged: every page |');
console.log('| --- | --- | --- | --- | --- |');

for (const mib of SIZES) {
  const dir = mkdtempSync(join(tmpdir(), 'bench-paged-'));
  try {
    const backing = new FileReplicaClient(join(dir, 'replica'));
    const rows = await replicate(dir, backing, mib);
    const client = network(backing, LATENCY_MS, BANDWIDTH_MBPS);

    // Full restore
    const fullPath = join(dir, 'full.db');
    const full = new Replica(fullPath, client);
    let start = performance.now();
    await full.restore({ outputPath: fullPath });
    const fullMs = performance.now() - start;
    full.close();
    const size = (await stat(fullPath)).size;
    await rm(fullPath, { force: true });

    // Paged restore, queried from another thread as soon as it resolves
    const pagedPath = join(dir, 'paged.db');
    const paged = new Replica(pagedPath, client);
    const worker = queryWorker();
    await worker.ready;
    start = performance.now();
    const result = await paged.restorePaged();
    if (!result.paged) throw new Error('the paged VFS cannot be used: build it with pnpm test:paged-vfs');
    const openMs = performance.now() - start;
    await worker.query(databaseUri(pagedPath), 1 + Math.floor(Math.random() * rows));
    const firstMs = performance.now() - start;
    await result.filled;
    const filledMs = performance.now() - start;
    await worker.close();
    paged.close();

    console.log(
      `| ${(size / 1024 / 1024).toFixed(0)} MiB | ${seconds(fullMs)} | ${seconds(openMs)} | ${seconds(firstMs)} | ${seconds(filledMs)} |`,
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

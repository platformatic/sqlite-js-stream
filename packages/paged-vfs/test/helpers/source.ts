import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { TestContext } from 'node:test';
import { Worker } from 'node:worker_threads';

export interface Row {
  id: number;
  v: string;
}

/** A database whose pages are served to the paged VFS. */
export interface Source {
  pageSize: number;
  pageCount: number;
  rows: Row[];
  page(pgno: number): Uint8Array;
}

export interface ReadResult {
  rows?: Record<string, unknown>[];
  error?: { message: string; errcode?: number };
}

export function tempDir(t: TestContext): string {
  const dir = mkdtempSync(join(tmpdir(), 'paged-vfs-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

/** Creates a rollback-journal database, complete in its file once closed. */
export function createSource(dir: string, count = 2000): Source {
  const path = join(dir, 'source.db');
  const db = new DatabaseSync(path);

  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  const insert = db.prepare('INSERT INTO t (v) VALUES (?)');
  db.exec('BEGIN');

  for (let i = 1; i <= count; i++) {
    insert.run(`value-${i}-${'x'.repeat(200)}`);
  }

  db.exec('COMMIT');

  const { page_size: pageSize } = db.prepare('PRAGMA page_size').get() as { page_size: number };
  const { page_count: pageCount } = db.prepare('PRAGMA page_count').get() as { page_count: number };
  const rows = db.prepare('SELECT id, v FROM t ORDER BY id').all().map(row => ({ ...row }) as unknown as Row);
  db.close();

  const bytes = readFileSync(path);
  return {
    pageSize,
    pageCount,
    rows,
    page: pgno => bytes.subarray((pgno - 1) * pageSize, pgno * pageSize),
  };
}

/** Runs statements on a database in a worker thread. */
export function read(workerData: { uri: string; exec?: string[]; query?: string }): Promise<ReadResult> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL('./reader.ts', import.meta.url), { workerData });
    worker.once('message', resolve);
    worker.once('error', reject);
  });
}

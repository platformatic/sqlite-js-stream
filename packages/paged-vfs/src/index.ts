/**
 * @platformatic/sqlite-js-stream-paged-vfs — a SQLite VFS that reads the pages
 * of a database on demand.
 *
 * A registered database is a sparse file with the final size of the database,
 * whose pages are all missing. When SQLite reads a missing page, the VFS calls
 * the `onRequest` callback of the database and blocks the reading thread until
 * the application fills the page with {@link PagedDatabase.fill}. The
 * application fills the other pages in the background; once every page is
 * present, reads are local only.
 *
 * The callback runs on the thread which registered the database. That thread
 * must never read the database while it is incomplete, as it would wait for
 * itself: such reads fail instead. Read it from another thread (for example a
 * worker) through {@link databaseUri}.
 *
 * Every connection that writes to a registered database must use the VFS, so
 * that a page written by SQLite is never overwritten by an older version of it.
 */

import { realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { pathToFileURL } from 'node:url';

export interface RegisterOptions {
  /** The path of the database file. It is created, or truncated, to `pageCount` pages. */
  path: string;
  pageSize: number;
  pageCount: number;
  /** How long a read waits for a missing page before failing, in milliseconds. Defaults to 30 seconds. */
  timeout?: number;
}

export interface PagedDatabase {
  readonly pageSize: number;
  /** The number of pages of the database, which only changes when SQLite truncates it. */
  readonly pageCount: number;
  /** The number of missing pages. */
  readonly missing: number;
  /** Whether every page is present. The database is then unregistered, and reads are local only. */
  readonly complete: boolean;
  /** Writes a page unless it is already present. Returns whether it was written. */
  fill(pgno: number, page: Uint8Array): boolean;
  /** Fails the reads waiting for a page. The next read of the page requests it again. */
  fail(pgno: number): void;
  /** The numbers of the missing pages, in ascending order. */
  missingPages(): Uint32Array;
  /** Unregisters the database. Reads of missing pages fail from now on. */
  close(): void;
}

interface Binding {
  isLoaded(): boolean;
  vfsName(): string;
  register(options: RegisterOptions, onRequest: (pgno: number) => void): PagedDatabase;
}

/** The name of the VFS, for the `vfs` parameter of a SQLite URI. */
export const VFS_NAME = 'sqlite-js-stream-paged';

const require = createRequire(import.meta.url);
let loaded: Binding | null = null;

function platform(): string {
  const { platform, arch } = process;

  if (platform !== 'linux') {
    return `${platform}-${arch}`;
  }

  const report = process.report.getReport() as { header?: { glibcVersionRuntime?: string } };
  return `${platform}-${arch}-${report.header?.glibcVersionRuntime ? 'gnu' : 'musl'}`;
}

/**
 * Returns the path of the native library: the local build, if any, or the one
 * of the package for this platform.
 */
export function libraryPath(): string {
  const name = `paged-vfs.${platform()}.node`;

  for (const candidate of [join(import.meta.dirname, '..', name), `@platformatic/sqlite-js-stream-paged-vfs-${platform()}/${name}`]) {
    try {
      return realpathSync(require.resolve(candidate));
    } catch {
      // Try the next one
    }
  }

  throw new Error(`The paged VFS is not available for ${platform()}: "${name}" was not found.`);
}

function binding(): Binding {
  if (loaded) {
    return loaded;
  }

  // The addon and the extension must be the same file, so that they share their state
  const path = libraryPath();
  const native = require(path) as Binding;

  if (!native.isLoaded()) {
    const db = new DatabaseSync(':memory:', { allowExtension: true });

    try {
      db.loadExtension(path);
    } finally {
      db.close();
    }

    if (!native.isLoaded()) {
      throw new Error(`SQLite loaded "${path}" as a different library than Node.js: the paged VFS cannot work on this platform.`);
    }
  }

  loaded = native;
  return native;
}

/**
 * Loads the native library, and registers the VFS in the SQLite of node:sqlite.
 * Idempotent. Fails when the permission model of Node.js is enabled, as it
 * forbids SQLite extensions.
 */
export function load(): void {
  binding();
}

/** Returns the SQLite URI which opens a database with the VFS. */
export function databaseUri(path: string): string {
  return `${pathToFileURL(path).href}?vfs=${VFS_NAME}`;
}

/**
 * Creates (or truncates) the database file to `pageCount` pages, all missing,
 * and registers it. `onRequest` is called on the current thread with the
 * number of each page SQLite waits for. Loads the VFS if needed.
 */
export function register(options: RegisterOptions, onRequest: (pgno: number) => void): PagedDatabase {
  return binding().register(options, onRequest);
}

/**
 * PagedRestore restores a database page by page, on demand, with the SQLite
 * VFS of `@platformatic/sqlite-js-stream-paged-vfs` (an optional peer
 * dependency, loaded only here).
 *
 * The database file is created sparse, at its final size, with every page
 * missing, and registered with the VFS. Connections that open it through the
 * VFS query it right away: a read of a missing page asks this class for it,
 * and waits until it is filled. Meanwhile every page is filled in the
 * background, until the database is complete and the VFS steps aside.
 *
 * Each page is filled with its newest version in the restore plan. The page
 * indexes of the plan's files say which file holds it, at which offset, so a
 * page is one ranged read away. A request reads the page with the next ones of
 * the same file (read-ahead); the background fill reads large ranges, a few at
 * a time. The VFS never fills a page SQLite wrote meanwhile, so the order of
 * the reads does not matter.
 *
 * Every page the replica's own connection reads while the database is
 * incomplete must be present: it runs on the thread that serves the requests,
 * where a read of a missing page fails instead of waiting. That connection
 * reads only the schema, which is filled before the restore resolves. The
 * replica reads the other pages from the file, after {@link PagedRestore.load}.
 */

import { open, rm, writeFile } from 'node:fs/promises';

import { ByteReader, decodePageData, parsePageIndex, type FileInfo } from './ltx/index.ts';
import { fetchLtxHeader, fetchPageIndexData, type ReplicaClient } from './replica-client.ts';
import { readAll } from './util/stream.ts';
import type { Logger } from './util/logger.ts';
import {
  DatabaseError,
  ErrPagedRestoreClosed,
  ErrPagedVfsUnavailable,
  LTXFormatError,
  PageSizeMismatchError,
} from './errors.ts';

/** The database API of `@platformatic/sqlite-js-stream-paged-vfs`. */
export interface PagedVfsDatabase {
  readonly missing: number;
  readonly complete: boolean;
  fill(pgno: number, page: Uint8Array): boolean;
  fail(pgno: number): void;
  close(): void;
}

/** The module API of `@platformatic/sqlite-js-stream-paged-vfs`. */
export interface PagedVfs {
  load(): void;
  databaseUri(path: string): string;
  register(
    options: { path: string; pageSize: number; pageCount: number; timeout?: number },
    onRequest: (pgno: number) => void,
  ): PagedVfsDatabase;
}

const PagedVfsPackage = '@platformatic/sqlite-js-stream-paged-vfs';

/**
 * LoadPagedVfs imports the paged VFS and registers it in the SQLite of
 * `node:sqlite`, or throws {@link ErrPagedVfsUnavailable}.
 */
export async function loadPagedVfs(): Promise<PagedVfs> {
  let vfs: PagedVfs;

  try {
    vfs = (await import(PagedVfsPackage)) as PagedVfs;
  } catch (err) {
    throw new ErrPagedVfsUnavailable(`${PagedVfsPackage} is not installed`, { cause: err });
  }

  try {
    vfs.load();
  } catch (err) {
    throw new ErrPagedVfsUnavailable(`cannot load the paged VFS: ${(err as Error).message}`, { cause: err });
  }

  return vfs;
}

export interface PagedRestoreStartOptions {
  vfs: PagedVfs;
  client: ReplicaClient;
  /** The restore plan, from {@link calcRestorePlan}. */
  plan: FileInfo[];
  /** The database path. Any file there is replaced. */
  path: string;
  /** A file which exists while the database is incomplete. */
  marker: string;
  logger: Logger;
  /** Pages read with each requested page. Defaults to 32. */
  readAhead?: number;
  /** How long a read waits for a missing page, in milliseconds. Defaults to the VFS's 30 seconds. */
  timeout?: number;
  /** Called when a page requested by a read is filled. */
  onMiss?: (pgno: number, pages: number, durationMs: number) => void;
}

/** A ranged read of some pages of one file of the plan. */
interface Run {
  file: number;
  start: number;
  end: number;
  pages: number[];
}

// The background fill reads ranges of at most this many bytes, possibly over
// frames of pages superseded by later files when they are at most maxGap long.
const BackgroundRunBytes = 4 * 1024 * 1024;
const BackgroundGap = 256 * 1024;
const BackgroundConcurrency = 4;
const IndexConcurrency = 8;
const MissAttempts = 3;
const MaxRetryDelay = 30_000;

export class PagedRestore {
  readonly path: string;
  readonly pageSize: number;
  readonly pageCount: number;
  /** Resolves once every page is present; rejects with {@link ErrPagedRestoreClosed} if closed before. */
  readonly filled: Promise<void>;

  private readonly client: ReplicaClient;
  private readonly plan: FileInfo[];
  private readonly marker: string;
  private readonly logger: Logger;
  private readonly readAhead: number;
  private readonly onMiss: PagedRestoreStartOptions['onMiss'];
  // For each page: the index in the plan of the file with its newest version
  // (-1 if none has it), and the offset and size of its frame there.
  private readonly fileOf: Int32Array;
  private readonly offsetOf: Float64Array;
  private readonly sizeOf: Uint32Array;
  // Pages known to be present: filled here, or found present by the VFS.
  private readonly present: Uint8Array;
  // Reads in progress, by page.
  private readonly pending = new Map<number, Promise<void>>();
  // Pages requested by reads, being read.
  private readonly requested = new Set<number>();
  private db: PagedVfsDatabase | null = null;
  private done = false;
  private closed = false;
  private resolveFilled!: () => void;
  private rejectFilled!: (err: Error) => void;

  private constructor(opts: PagedRestoreStartOptions, pageSize: number, pageCount: number) {
    this.path = opts.path;
    this.pageSize = pageSize;
    this.pageCount = pageCount;
    this.client = opts.client;
    this.plan = opts.plan;
    this.marker = opts.marker;
    this.logger = opts.logger;
    this.readAhead = Math.max(1, opts.readAhead ?? 32);
    this.onMiss = opts.onMiss;
    this.fileOf = new Int32Array(pageCount + 1).fill(-1);
    this.offsetOf = new Float64Array(pageCount + 1);
    this.sizeOf = new Uint32Array(pageCount + 1);
    this.present = new Uint8Array(pageCount + 1);
    this.filled = new Promise((resolve, reject) => {
      this.resolveFilled = resolve;
      this.rejectFilled = reject;
    });
    // Callers may never wait for it
    this.filled.catch(() => {});
  }

  /**
   * Start creates the database file with every page missing, registers it
   * with the VFS, and fills page 1 and the pages of the schema. Requests for
   * pages are served from then on; call {@link fill} to fill the others.
   */
  static async start(opts: PagedRestoreStartOptions): Promise<PagedRestore> {
    const { client, plan } = opts;
    const [first, last] = await Promise.all([fetchLtxHeader(client, plan[0]), fetchLtxHeader(client, plan[plan.length - 1])]);
    if (first.pageSize !== last.pageSize) {
      throw new PageSizeMismatchError(`mismatched page sizes: ${first.pageSize} != ${last.pageSize}`);
    }

    const restore = new PagedRestore(opts, first.pageSize, last.commit);
    await restore.readPageIndexes();

    // Created first, so that it outlives anything left of the database by a crash
    await writeFile(opts.marker, `${plan[plan.length - 1].maxTXID}\n`);
    for (const suffix of ['', '-wal', '-shm', '-wal.tmp']) {
      await rm(`${opts.path}${suffix}`, { force: true });
    }

    restore.db = opts.vfs.register(
      { path: opts.path, pageSize: restore.pageSize, pageCount: restore.pageCount, timeout: opts.timeout },
      (pgno) => restore.request(pgno),
    );

    try {
      await restore.loadSchema();
    } catch (err) {
      restore.close();
      throw err;
    }
    return restore;
  }

  /** Missing pages, according to the VFS. */
  get missing(): number {
    return this.done || this.db === null ? 0 : this.db.missing;
  }

  /** Whether the restore is done: every page is present, or it was closed. */
  get finished(): boolean {
    return this.done || this.closed;
  }

  /** Whether `info` is a file the restore reads, until it is done. */
  reads(info: FileInfo): boolean {
    if (this.finished) return false;
    return this.plan.some((f) => f.level === info.level && f.minTXID === info.minTXID && f.maxTXID === info.maxTXID);
  }

  /**
   * Load makes a page present in the database file, before reading it from
   * the file directly. Returns undefined if it is already present.
   */
  load(pgno: number): Promise<void> | undefined {
    if (this.finished || pgno < 1 || pgno > this.pageCount || this.present[pgno] === 1) return undefined;
    return this.pending.get(pgno) ?? this.readFor(pgno, false);
  }

  /**
   * Fill fills every missing page in the background, retrying failed reads,
   * until the database is complete or the restore is closed. If the database
   * cannot be completed, it closes the restore with the error.
   */
  async fill(): Promise<void> {
    let delay = 1000;

    try {
      while (!this.finished) {
        let failed = false;
        const runs = this.backgroundRuns();
        const worker = async (): Promise<void> => {
          for (let next = runs.next(); !next.done && !this.finished; next = runs.next()) {
            try {
              await this.track(next.value.pages, this.read(next.value));
            } catch (err) {
              if (this.finished) return;
              failed = true;
              this.logger.warn('cannot read pages for a paged restore', { path: this.path, err });
            }
          }
        };
        await Promise.all(Array.from({ length: BackgroundConcurrency }, worker));

        // The pass skipped the pages being read for a request
        await Promise.allSettled([...this.pending.values()]);
        this.finishIfComplete();
        if (this.finished) return;

        if (failed) {
          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(delay * 2, MaxRetryDelay);
        } else if (this.present.indexOf(0, 1) === -1) {
          throw new DatabaseError(`a paged restore of ${this.path} filled every page, yet ${this.missing} are missing`);
        }
      }
    } catch (err) {
      this.close(err as Error);
    }
  }

  /**
   * Close stops the restore, and rejects {@link filled} with `err`, or with
   * {@link ErrPagedRestoreClosed}. The database stays incomplete, and reads of
   * its missing pages fail.
   */
  close(err?: Error): void {
    if (this.finished) return;
    this.closed = true;
    this.db?.close();
    this.pending.clear();
    this.rejectFilled(err ?? new ErrPagedRestoreClosed(this.path));
  }

  /** Records the read of `pages` in progress, until it settles. */
  private track(pages: number[], read: Promise<void>): Promise<void> {
    const promise = read.finally(() => {
      for (const pgno of pages) {
        if (this.pending.get(pgno) === promise) this.pending.delete(pgno);
      }
    });
    for (const pgno of pages) this.pending.set(pgno, promise);
    return promise;
  }

  /** Reads the page indexes of the plan, keeping the newest version of each page. */
  private async readPageIndexes(): Promise<void> {
    let next = 0;
    const worker = async (): Promise<void> => {
      for (let i = next++; i < this.plan.length; i = next++) {
        const data = await fetchPageIndexData(this.client, this.plan[i]);
        parsePageIndex(new ByteReader(data, 0), null, (pgno, offset, size) => {
          if (pgno <= this.pageCount && i > this.fileOf[pgno]) {
            this.fileOf[pgno] = i;
            this.offsetOf[pgno] = offset;
            this.sizeOf[pgno] = size;
          }
        });
      }
    };
    await Promise.all(Array.from({ length: Math.min(IndexConcurrency, this.plan.length) }, worker));
  }

  /**
   * Fills the pages SQLite reads to open the database: page 1, and the pages
   * of the schema table (sqlite_master), a b-tree rooted at page 1, with the
   * overflow pages of its records.
   */
  private async loadSchema(): Promise<void> {
    const page1 = await this.readFromFile(1);
    const usable = this.pageSize - page1[20];
    let level: Array<{ pgno: number; overflow: boolean; data: Uint8Array }> = [{ pgno: 1, overflow: false, data: page1 }];
    const seen = new Set([1]);

    while (level.length > 0) {
      const children: Array<{ pgno: number; overflow: boolean }> = [];
      for (const page of level) {
        const next = page.overflow ? overflowNext(page.data) : schemaChildren(page.data, page.pgno, usable);
        for (const child of next) {
          if (child.pgno < 1 || child.pgno > this.pageCount || seen.has(child.pgno)) {
            throw new DatabaseError(`corrupt schema: page ${page.pgno} refers to page ${child.pgno}`);
          }
          seen.add(child.pgno);
          children.push(child);
        }
      }
      level = await Promise.all(children.map(async (c) => ({ ...c, data: await this.readFromFile(c.pgno) })));
    }
  }

  /** Makes a page present, and returns its content. */
  private async readFromFile(pgno: number): Promise<Uint8Array> {
    await this.load(pgno);
    const page = new Uint8Array(this.pageSize);
    const fh = await open(this.path, 'r');
    try {
      const { bytesRead } = await fh.read(page, 0, this.pageSize, (pgno - 1) * this.pageSize);
      if (bytesRead !== this.pageSize) throw new DatabaseError(`cannot read page ${pgno} of ${this.path}`);
    } finally {
      await fh.close();
    }
    return page;
  }

  /** Serves a request of the VFS for a page, on the thread which registered the database. */
  private request(pgno: number): void {
    if (this.finished || this.requested.has(pgno)) return;
    // A page being read by the background fill is read again, in a smaller read
    this.readFor(pgno, true).catch(() => {});
  }

  /**
   * Reads a page with the next ones of the same file, retrying a few times.
   * A page requested by the VFS is failed if it cannot be read, so that the
   * read waiting for it fails instead of waiting until it times out.
   */
  private readFor(pgno: number, requested: boolean): Promise<void> {
    const run = this.missRun(pgno);
    const start = performance.now();
    if (requested) this.requested.add(pgno);

    const read = (async () => {
      for (let attempt = 1; ; attempt++) {
        try {
          if (run === null) {
            this.fillPage(pgno, new Uint8Array(this.pageSize));
          } else {
            await this.read(run);
          }
          if (requested) this.onMiss?.(pgno, run?.pages.length ?? 1, performance.now() - start);
          return;
        } catch (err) {
          if (this.closed) throw new ErrPagedRestoreClosed(this.path);
          if (attempt >= MissAttempts) {
            this.logger.warn('cannot read a page for a paged restore', { path: this.path, pgno, err });
            if (requested) this.db?.fail(pgno);
            throw err;
          }
          await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
        }
      }
    })().finally(() => {
      if (requested) this.requested.delete(pgno);
    });

    return this.track(run?.pages ?? [pgno], read);
  }

  /**
   * The read of a page and of the next pages of the same file, up to the
   * read-ahead, or null if no file has the page. The frames of the pages
   * already present are read too, but they are not filled.
   */
  private missRun(pgno: number): Run | null {
    const file = this.fileOf[pgno];
    if (file < 0) return null;

    const run: Run = { file, start: this.offsetOf[pgno], end: this.offsetOf[pgno] + this.sizeOf[pgno], pages: [pgno] };
    let end = run.end;
    for (let p = pgno + 1, frames = 1; p <= this.pageCount && frames < this.readAhead; p++, frames++) {
      if (this.fileOf[p] !== file || this.offsetOf[p] !== end) break;
      end += this.sizeOf[p];
      if (this.present[p] === 0) {
        run.pages.push(p);
        run.end = end;
      }
    }
    return run;
  }

  /**
   * Yields the reads of the background fill, in page order: for each file,
   * ranges of the frames of the pages it has the newest version of, which are
   * neither present nor being read. Pages no file has are filled with zeros,
   * as a full restore leaves them.
   */
  private *backgroundRuns(): Generator<Run> {
    const open = new Map<number, Run>();

    for (let pgno = 1; pgno <= this.pageCount && !this.finished; pgno++) {
      if (this.present[pgno] === 1 || this.pending.has(pgno)) continue;

      const file = this.fileOf[pgno];
      if (file < 0) {
        this.fillPage(pgno, new Uint8Array(this.pageSize));
        continue;
      }

      const offset = this.offsetOf[pgno];
      const end = offset + this.sizeOf[pgno];
      const run = open.get(file);
      if (run !== undefined && offset >= run.end && offset - run.end <= BackgroundGap && end - run.start <= BackgroundRunBytes) {
        run.pages.push(pgno);
        run.end = end;
        continue;
      }
      if (run !== undefined) yield run;
      open.set(file, { file, start: offset, end, pages: [pgno] });
    }

    yield* open.values();
  }

  /** Reads a run, and fills its pages. */
  private async read(run: Run): Promise<void> {
    const info = this.plan[run.file];
    const stream = await this.client.openLtxFile({
      level: info.level,
      minTXID: info.minTXID,
      maxTXID: info.maxTXID,
      offset: run.start,
      size: run.end - run.start,
    });
    const bytes = await readAll(stream);
    if (bytes.length !== run.end - run.start) {
      throw new LTXFormatError(`short read of ltx file ${info.minTXID}-${info.maxTXID}: ${bytes.length} of ${run.end - run.start} bytes`);
    }

    // Filling copies the page, so one buffer serves the whole run
    const page = new Uint8Array(this.pageSize);
    for (const pgno of run.pages) {
      if (this.finished) return;
      if (this.present[pgno] === 1) continue;
      const offset = this.offsetOf[pgno] - run.start;
      const { header, data } = decodePageData(bytes.subarray(offset, offset + this.sizeOf[pgno]), page);
      if (header.pgno !== pgno) {
        throw new LTXFormatError(`unexpected page ${header.pgno} at the offset of page ${pgno}`);
      }
      if (data.length !== this.pageSize) {
        throw new PageSizeMismatchError(`page ${pgno} has ${data.length} bytes instead of ${this.pageSize}`);
      }
      this.fillPage(pgno, data);
    }
  }

  private fillPage(pgno: number, data: Uint8Array): void {
    if (this.finished || this.db === null) return;
    this.db.fill(pgno, data);
    this.present[pgno] = 1;
    this.finishIfComplete();
  }

  private finishIfComplete(): void {
    if (this.finished || this.db === null || !this.db.complete) return;
    this.done = true;
    this.pending.clear();
    rm(this.marker, { force: true }).then(
      () => this.resolveFilled(),
      (err) => this.rejectFilled(err),
    );
  }
}

/**
 * The pages a page of the schema table refers to: the children of an
 * interior page, or the first overflow page of each record of a leaf page.
 * See https://www.sqlite.org/fileformat2.html#b_tree_pages.
 */
function schemaChildren(page: Uint8Array, pgno: number, usable: number): Array<{ pgno: number; overflow: boolean }> {
  const header = pgno === 1 ? 100 : 0;
  const view = new DataView(page.buffer, page.byteOffset, page.byteLength);
  const cells = view.getUint16(header + 3);
  const out: Array<{ pgno: number; overflow: boolean }> = [];

  switch (page[header]) {
    case 0x05: // Interior table page: a left child per cell, then the right-most child
      for (let i = 0; i < cells; i++) {
        out.push({ pgno: view.getUint32(view.getUint16(header + 12 + 2 * i)), overflow: false });
      }
      out.push({ pgno: view.getUint32(header + 8), overflow: false });
      return out;
    case 0x0d: {
      // Leaf table page: the payload of a record which does not fit continues in overflow pages
      const maxLocal = usable - 35;
      const minLocal = Math.floor(((usable - 12) * 32) / 255) - 23;
      for (let i = 0; i < cells; i++) {
        let offset = view.getUint16(header + 8 + 2 * i);
        const [payload, n] = readVarint(page, offset);
        offset += n;
        offset += readVarint(page, offset)[1]; // rowid
        if (payload > maxLocal) {
          const k = minLocal + ((payload - minLocal) % (usable - 4));
          const local = k <= maxLocal ? k : minLocal;
          out.push({ pgno: view.getUint32(offset + local), overflow: true });
        }
      }
      return out;
    }
    default:
      throw new DatabaseError(`corrupt schema: page ${pgno} is not a table b-tree page`);
  }
}

/** The next page of an overflow chain, if any. */
function overflowNext(page: Uint8Array): Array<{ pgno: number; overflow: boolean }> {
  const next = new DataView(page.buffer, page.byteOffset, page.byteLength).getUint32(0);
  return next === 0 ? [] : [{ pgno: next, overflow: true }];
}

/** Reads a SQLite varint: returns its value and its length. */
function readVarint(b: Uint8Array, offset: number): [number, number] {
  let value = 0;
  for (let i = 0; i < 8; i++) {
    const byte = b[offset + i];
    value = value * 128 + (byte & 0x7f);
    if (byte < 0x80) return [value, i + 1];
  }
  return [value * 256 + b[offset + 8], 9];
}

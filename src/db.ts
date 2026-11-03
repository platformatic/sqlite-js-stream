/**
 * LocalDatabase is the internal engine that reads a SQLite database and its
 * `-wal` file and writes LTX files into a local cache (`<path>-litestream/ltx/`).
 *
 * It is deliberately NOT part of the public API: `Replica`/`Store` take a
 * database `path` and read directly, so the library is agnostic to whatever
 * SQLite driver the application uses. To keep snapshots consistent, it holds a
 * short-lived **read-only** `node:sqlite` read transaction that pins the WAL
 * (SQLite will not checkpoint/truncate the WAL past an active reader), while
 * reading the database + WAL files directly.
 */

import { DatabaseSync } from 'node:sqlite';
import { rmSync, writeFileSync } from 'node:fs';
import { open, readFile, readdir, rename, rm, stat, type FileHandle } from 'node:fs/promises';
import nodePath from 'node:path';

import {
  Decoder,
  Encoder,
  HeaderFlagNoChecksum,
  HeaderSize,
  formatFilename,
  lockPgno,
  parseFilename,
  type FileInfo,
  type Header,
} from './ltx/index.ts';
import { FileSink } from './util/fs.ts';
import {
  PrevFrameMismatchError,
  WALFrameHeaderSize,
  WALHeaderSize,
  WALReader,
  WALEOFError,
} from './wal/index.ts';
import { SnapshotLevel } from './replica-client.ts';
import { mkdirAll } from './util/fs.ts';
import { DatabaseError } from './errors.ts';

/**
 * ReaderConnection is the minimal SQLite connection used to pin a WAL read
 * snapshot during sync/snapshot. `node:sqlite`'s `DatabaseSync` satisfies it;
 * other drivers (e.g. libsql with an encryption key) can be plugged in via
 * {@link OpenReader}.
 */
export interface ReaderConnection {
  exec(sql: string): void;
  prepare(sql: string): { get(): unknown };
  close(): void;
}

/**
 * OpenReader opens a read-only {@link ReaderConnection} for a database path.
 * Defaults to a read-only `node:sqlite` connection.
 */
export type OpenReader = (path: string) => ReaderConnection;

/**
 * CheckpointConnection is the minimal read-write SQLite connection used by
 * {@link runCheckpoint} to run `PRAGMA wal_checkpoint`. A
 * `node:sqlite` `DatabaseSync` satisfies it. It must not have an open
 * transaction.
 */
export interface CheckpointConnection {
  prepare(sql: string): { get(): unknown };
}

/**
 * CheckpointMode is a `wal_checkpoint` mode that resets the WAL. Both modes
 * guarantee that the next write starts a new WAL; `TRUNCATE` also truncates
 * the `-wal` file to zero bytes.
 */
export type CheckpointMode = 'RESTART' | 'TRUNCATE';

/**
 * CheckpointToken describes the position of the last sync, for
 * {@link runCheckpoint}. It is plain JSON, so it can be sent to the thread or
 * process that owns the writing connection.
 */
export interface CheckpointToken {
  /** Path of the database. */
  path: string;
  /** Local TXID of the last sync, as a decimal string. */
  txid: string;
  /** WAL salts recorded by the last sync. */
  salt1: number;
  salt2: number;
  /**
   * Number of WAL frames the last sync covered; 0 when it does not cover a
   * known WAL, in which case the checkpoint cannot keep the next sync
   * incremental.
   */
  frames: number;
}

/**
 * The record left by a checkpoint that reset the WAL right after a sync: the
 * next WAL (the one with `nextSalt1`) holds only changes on top of `txid`.
 */
interface CheckpointRecord {
  txid: bigint;
  salt1: number;
  salt2: number;
  nextSalt1: number;
}

/** Path of the checkpoint record of the database at `path`. */
function checkpointRecordPath(path: string): string {
  return nodePath.join(`${path}-litestream`, 'checkpoint');
}

/**
 * RunCheckpoint checkpoints the WAL of `token.path` with `mode` through `conn`,
 * the connection that writes the database (with no open transaction), and,
 * if the WAL held exactly the frames of the sync described by `token`,
 * records that the next WAL continues from it. Returns whether it recorded it.
 *
 * It is synchronous, so no write on the calling thread can land between the
 * checkpoint and the record. No sync of the database may run between the
 * call that produced `token` and this one.
 *
 * The checkpoint runs as `RESTART` first: its `log` column (the number of
 * frames in the WAL) proves that no commit landed between the sync and the
 * checkpoint, and a complete `RESTART` guarantees that the next write starts
 * a new WAL. SQLite gives that WAL the old salt-1 plus one when the connection
 * that starts it has reset the WAL before (as `conn` just did); the next sync
 * accepts the new WAL only with that salt-1. Any other reset (another
 * checkpoint, or a WAL started by a different connection, which picks random
 * salts) falls back to a snapshot. `TRUNCATE` then truncates the `-wal` file.
 */
export function runCheckpoint(
  conn: CheckpointConnection,
  token: CheckpointToken,
  mode: CheckpointMode = 'TRUNCATE',
): boolean {
  if (mode !== 'RESTART' && mode !== 'TRUNCATE') {
    throw new DatabaseError(`unsupported checkpoint mode: ${mode}`);
  }
  const recordPath = checkpointRecordPath(token.path);
  rmSync(recordPath, { force: true });

  const r = conn.prepare('PRAGMA wal_checkpoint(RESTART)').get() as {
    busy: number;
    log: number;
    checkpointed: number;
  };
  const complete =
    token.frames > 0 && r.busy === 0 && r.log === token.frames && r.checkpointed === token.frames;

  if (complete) {
    const record = {
      txid: token.txid,
      salt1: token.salt1,
      salt2: token.salt2,
      nextSalt1: (token.salt1 + 1) >>> 0,
    };
    writeFileSync(recordPath, `${JSON.stringify(record)}\n`);
  }

  if (mode === 'TRUNCATE') {
    // Whatever the outcome, the next WAL is the one recorded above (or a
    // later one, which falls back to a snapshot).
    conn.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  }
  return complete;
}

function defaultOpenReader(path: string): ReaderConnection {
  return new DatabaseSync(path, { readOnly: true });
}

/** Reads the page size from the SQLite database header (bytes 16-17). */
function headerPageSize(buf: Uint8Array): number {
  const raw = buf.length >= 18 ? (buf[16] << 8) | buf[17] : 0;
  if (raw === 1) return 65536;
  return raw || 4096;
}

/**
 * LocalDatabase reads a database + WAL from disk, tracks the local replicated
 * position, and builds LTX files in the local cache.
 */
export class LocalDatabase {
  readonly path: string;
  readonly metaPath: string;
  readonly pageSize: number;
  private conn: ReaderConnection | null = null;
  private openReader: OpenReader;

  private constructor(path: string, metaPath: string, pageSize: number, openReader: OpenReader) {
    this.path = path;
    this.metaPath = metaPath;
    this.pageSize = pageSize;
    this.openReader = openReader;
  }

  /**
   * Open opens a local database reader for `path`. It does not create the
   * database — the application owns it. `pageSize` is read from the database
   * header; a missing file falls back to the default page size. `openReader`
   * overrides the connection used to pin read snapshots.
   */
  static async open(path: string, openReader: OpenReader = defaultOpenReader): Promise<LocalDatabase> {
    const metaPath = `${path}-litestream`;
    await mkdirAll(nodePath.join(metaPath, 'ltx', '0'));

    let pageSize = 4096;
    try {
      const fh = await open(path, 'r');
      const buf = new Uint8Array(18);
      await fh.read(buf, 0, 18, 0);
      await fh.close();
      pageSize = headerPageSize(buf);
    } catch {
      /* database not present yet; use default page size */
    }

    return new LocalDatabase(path, metaPath, pageSize, openReader);
  }

  /** WalPath returns the path to the database's WAL file. */
  walPath(): string {
    return `${this.path}-wal`;
  }

  /**
   * BaseTXIDPath returns the path of the file recording the TXID a restore left
   * the database at, so that the next sync continues the replica's sequence.
   */
  baseTXIDPath(): string {
    return nodePath.join(this.metaPath, 'base-txid');
  }

  /** Returns the TXID recorded by a restore, or 0 if there is none. */
  private async baseTXID(): Promise<bigint> {
    try {
      return BigInt((await readFile(this.baseTXIDPath(), 'utf8')).trim());
    } catch {
      return 0n;
    }
  }

  /**
   * CheckpointPath returns the path of the file recording that a checkpoint
   * reset the WAL right after a sync (see {@link runCheckpoint}).
   */
  checkpointPath(): string {
    return checkpointRecordPath(this.path);
  }

  /** Returns the checkpoint record, or null if there is none. */
  private async checkpointRecord(): Promise<CheckpointRecord | null> {
    try {
      const r = JSON.parse(await readFile(this.checkpointPath(), 'utf8'));
      return { txid: BigInt(r.txid), salt1: r.salt1, salt2: r.salt2, nextSalt1: r.nextSalt1 };
    } catch {
      return null;
    }
  }

  /** LTXDir returns the local LTX root directory. */
  ltxDir(): string {
    return nodePath.join(this.metaPath, 'ltx');
  }

  /** LTXLevelDir returns the local LTX directory for a level. */
  ltxLevelDir(level: number): string {
    return nodePath.join(this.ltxDir(), String(level));
  }

  /** LTXPath returns the local path of a single LTX file. */
  ltxPath(level: number, minTXID: bigint, maxTXID: bigint): string {
    return nodePath.join(this.ltxLevelDir(level), formatFilename(minTXID, maxTXID));
  }

  /** Lists local level-0 LTX files, sorted by TXID. */
  async l0Files(): Promise<FileInfo[]> {
    return this.filesForLevel(0);
  }

  /** Lists local LTX files for any level, sorted by TXID. */
  async filesForLevel(level: number): Promise<FileInfo[]> {
    const dir = this.ltxLevelDir(level);
    const out: FileInfo[] = [];
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return out;
      throw err;
    }
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      let minTXID: bigint;
      let maxTXID: bigint;
      try {
        ({ minTXID, maxTXID } = parseFilename(entry.name));
      } catch {
        continue;
      }
      const st = await stat(nodePath.join(dir, entry.name));
      out.push({ level, minTXID, maxTXID, size: st.size, createdAt: st.mtimeMs });
    }
    out.sort((a, b) => (a.minTXID < b.minTXID ? -1 : a.minTXID > b.minTXID ? 1 : 0));
    return out;
  }

  /**
   * Pos returns the maximum TXID in the local cache, or the TXID recorded by a
   * restore if it is higher (0 if none).
   */
  async pos(): Promise<bigint> {
    const m = await this.maxFile();
    const base = await this.baseTXID();
    return m === null || m.maxTXID < base ? base : m.maxTXID;
  }

  /** Returns the local LTX file with the highest maxTXID across all levels. */
  private async maxFile(): Promise<FileInfo | null> {
    let best: FileInfo | null = null;
    for (let level = 0; level <= SnapshotLevel; level++) {
      for (const f of await this.filesForLevel(level)) {
        if (best === null || f.maxTXID > best.maxTXID) best = f;
      }
    }
    return best;
  }

  /**
   * Opens (and caches) a read-only connection used to hold a snapshot during
   * sync/snapshot. Not part of the public API; the DB file must exist.
   */
  private ensureConn(): ReaderConnection {
    if (this.conn === null) {
      this.conn = this.openReader(this.path);
      this.conn.exec('PRAGMA busy_timeout=5000');
    }
    return this.conn;
  }

  /**
   * Pins a WAL snapshot by starting a read transaction and issuing a read that
   * touches the database (a plain `SELECT 1` does not acquire the WAL read lock).
   * Rolls back if the read fails, so the next sync can start a new transaction.
   */
  private beginRead(): void {
    const conn = this.ensureConn();
    conn.exec('BEGIN');
    try {
      conn.prepare('SELECT count(*) FROM sqlite_master').get();
    } catch (err) {
      conn.exec('ROLLBACK');
      throw err;
    }
  }

  /** Releases the pinned read snapshot. */
  private commitRead(): void {
    if (this.conn !== null) {
      this.conn.exec('COMMIT');
    }
  }

  /** Current committed database size, in pages (as seen by the pinned reader). */
  private pageCount(): number {
    const r = this.ensureConn().prepare('PRAGMA page_count').get() as { page_count: number };
    return r.page_count;
  }

  /** Sync builds a level-0 LTX file and writes it to the local cache. */
  async sync(): Promise<FileInfo | null> {
    const pos = await this.pos();
    const txID = pos + 1n;

    this.beginRead();
    let dbHandle: FileHandle | null = null;
    try {
      const commit = this.pageCount();
      if (commit === 0) return null;

      const pageSize = this.pageSize;
      const lockPg = lockPgno(pageSize);

      dbHandle = await open(this.path, 'r');
      const walBuf = await this.readWalOrEmpty();

      // Determine the previous sync state to resume incremental reading.
      let prev = await this.prevSyncInfo();
      let snapshotting = prev === null;

      let reader: WALReader | null = null;
      let offset = WALHeaderSize;
      let salt1 = 0;
      let salt2 = 0;
      let pageMap = new Map<number, number>();
      let maxOffset = 0;
      let walCommit = 0;
      let prevCommit = 0;

      if (!snapshotting && prev !== null) {
        reader = makeResumeReader(walBuf, prev.walOffset + prev.walSize, prev.salt1, prev.salt2);
        if (reader === null && (await this.checkpointed(pos, prev))) {
          // The WAL was reset by runCheckpoint() right after the previous sync:
          // the database file holds the state at `pos`, and every frame of the
          // new WAL is a change on top of it.
          if (walBuf.length < WALHeaderSize) return null; // no new WAL yet
          reader = makeNextReader(walBuf, (await this.checkpointRecord())!.nextSalt1);
          if (reader !== null) {
            prev = { ...prev, walOffset: WALHeaderSize, walSize: 0, salt1: reader.salt1(), salt2: reader.salt2() };
          }
        }
        if (reader === null) {
          snapshotting = true; // WAL was reset; fall back to a snapshot base
        } else {
          const pm = reader.pageMap();
          pageMap = pm.pageIndex;
          maxOffset = pm.maxOffset;
          walCommit = pm.commit;
          offset = prev.walOffset + prev.walSize;
          salt1 = prev.salt1;
          salt2 = prev.salt2;
          prevCommit = prev.commit;
        }
      }

      const walSize = snapshotting ? 0 : maxOffset > 0 ? maxOffset - offset : 0;

      if (!snapshotting && walSize === 0) {
        return null; // no new WAL pages since the last sync
      }

      if (snapshotting) {
        // Read the WAL from the start to build a full page map for the snapshot.
        offset = WALHeaderSize;
        maxOffset = 0;
        try {
          const full = new WALReader(walBuf);
          const pm = full.pageMap();
          pageMap = pm.pageIndex;
          walCommit = pm.commit;
          maxOffset = pm.maxOffset;
          salt1 = full.salt1();
          salt2 = full.salt2();
        } catch (err) {
          if (!(err instanceof WALEOFError)) throw err;
        }
      }

      const finalCommit = Math.max(commit, walCommit);

      // Stream the LTX output directly to a temp file (no whole-file buffer).
      const dir = this.ltxLevelDir(0);
      await mkdirAll(dir);
      const filename = this.ltxPath(0, txID, txID);
      const tmp = `${filename}.tmp`;

      const fh = await open(tmp, 'w');
      const sink = new FileSink(fh);
      const enc = new Encoder(sink);
      try {
      enc.encodeHeader({
        version: 3,
        flags: HeaderFlagNoChecksum,
        pageSize,
        commit: finalCommit,
        minTXID: txID,
        maxTXID: txID,
        timestamp: BigInt(Date.now()),
        preApplyChecksum: 0n,
        walOffset: BigInt(offset),
        // A snapshot covers the WAL up to its last commit frame, so the next
        // sync resumes right after it (not after uncommitted frames).
        walSize: BigInt(snapshotting ? (maxOffset > 0 ? maxOffset - WALHeaderSize : 0) : walSize),
        walSalt1: salt1,
        walSalt2: salt2,
        nodeID: 0n,
      });

      if (snapshotting) {
        await this.writePagesFromDB(enc, pageSize, finalCommit, lockPg, pageMap, walBuf, dbHandle);
      } else {
        await this.writePagesFromWAL(enc, pageSize, finalCommit, prevCommit, lockPg, pageMap, walBuf, dbHandle);
      }
      enc.close();
      await sink.flush();
      await fh.close();

      await rename(tmp, filename);

      const size = (await stat(filename)).size;
      return { level: 0, minTXID: txID, maxTXID: txID, size, createdAt: Date.now() };
      } catch (err) {
        await fh.close().catch(() => {});
        await rm(tmp, { force: true }).catch(() => {});
        throw err;
      }
    } finally {
      if (dbHandle !== null) await dbHandle.close();
      this.commitRead();
    }
  }

  /** Writes a full snapshot: every committed page, preferring WAL pages. */
  private async writePagesFromDB(
    enc: Encoder,
    pageSize: number,
    commit: number,
    lockPg: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbHandle: FileHandle,
  ): Promise<void> {
    for (let pgno = 1; pgno <= commit; pgno++) {
      if (pgno === lockPg) continue;
      enc.encodePage({ pgno, flags: 0 }, await this.readPage(pgno, pageSize, pageMap, walBuf, dbHandle));
    }
  }

  /** Writes an incremental file: changed WAL pages + DB growth pages. */
  private async writePagesFromWAL(
    enc: Encoder,
    pageSize: number,
    commit: number,
    prevCommit: number,
    lockPg: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbHandle: FileHandle,
  ): Promise<void> {
    // Sorted page numbers present in the WAL change-set.
    const pgnos = [...pageMap.keys()].sort((a, b) => a - b);
    const lockPgno = lockPg;

    // Add DB growth pages (new pages beyond prevCommit not already in pageMap).
    if (commit > prevCommit) {
      for (let pgno = prevCommit + 1; pgno <= commit; pgno++) {
        if (pgno === lockPgno) continue;
        if (pageMap.has(pgno)) continue;
        pgnos.push(pgno);
      }
    }

    for (const pgno of pgnos) {
      if (pgno === lockPgno) continue;
      // Growth pages (not in pageMap) come directly from the database file.
      if (!pageMap.has(pgno)) {
        const page = new Uint8Array(pageSize);
        await readExact(dbHandle, page, (pgno - 1) * pageSize);
        enc.encodePage({ pgno, flags: 0 }, page);
        continue;
      }
      enc.encodePage({ pgno, flags: 0 }, await this.readPage(pgno, pageSize, pageMap, walBuf, dbHandle));
    }
  }

  /**
   * CheckpointToken describes the position of the last sync for
   * {@link runCheckpoint}. The caller must have synced first. It reads the
   * local cache only, so it can be called from a different thread than the
   * one running the checkpoint.
   */
  async checkpointToken(): Promise<CheckpointToken> {
    const pos = await this.pos();
    const prev = await this.prevSyncInfo();
    // The frames the last sync covered, all in the WAL with prev's salts.
    const end = prev === null ? 0 : prev.walOffset + prev.walSize;
    const frames = (end - WALHeaderSize) / (this.pageSize + WALFrameHeaderSize);
    return {
      path: this.path,
      txid: String(pos),
      salt1: prev?.salt1 ?? 0,
      salt2: prev?.salt2 ?? 0,
      frames: prev !== null && end > WALHeaderSize && Number.isInteger(frames) ? frames : 0,
    };
  }

  /**
   * Reports whether the WAL was reset by {@link runCheckpoint} right after the
   * sync that wrote the file at `pos` described by `prev`.
   */
  private async checkpointed(
    pos: bigint,
    prev: { salt1: number; salt2: number },
  ): Promise<boolean> {
    const r = await this.checkpointRecord();
    return r !== null && r.txid === pos && r.salt1 === prev.salt1 && r.salt2 === prev.salt2;
  }

  /**
   * Snapshot writes a full-database LTX file at `SnapshotLevel` (9) with
   * MinTXID=1 and MaxTXID equal to the current local position. Returns null if
   * a snapshot would cover no data (empty database).
   */
  async snapshot(): Promise<FileInfo | null> {
    const pos = await this.pos();
    const maxTXID = pos === 0n ? 1n : pos;

    this.beginRead();
    let dbHandle: FileHandle | null = null;
    try {
      const commit = this.pageCount();
      if (commit === 0) return null;

      const pageSize = this.pageSize;
      const lockPg = lockPgno(pageSize);
      dbHandle = await open(this.path, 'r');
      const walBuf = await this.readWalOrEmpty();

      // Build the committed WAL page map for the overlay (may be empty).
      let pageMap = new Map<number, number>();
      let walCommit = 0;
      let salt1 = 0;
      let salt2 = 0;
      if (walBuf.length > 0) {
        try {
          const full = new WALReader(walBuf);
          const pm = full.pageMap();
          pageMap = pm.pageIndex;
          walCommit = pm.commit;
          salt1 = full.salt1();
          salt2 = full.salt2();
        } catch (err) {
          if (!(err instanceof WALEOFError)) throw err;
        }
      }

      // The commit may only be present in the WAL (not yet checkpointed into
      // the main database header), so take the larger of the two.
      const finalCommit = Math.max(commit, walCommit);
      if (finalCommit === 0) return null;

      // Stream the LTX output directly to a temp file (no whole-file buffer).
      const dir = this.ltxLevelDir(9);
      await mkdirAll(dir);
      const filename = this.ltxPath(9, 1n, maxTXID);
      const tmp = `${filename}.tmp`;

      const fh = await open(tmp, 'w');
      const sink = new FileSink(fh);
      const enc = new Encoder(sink);
      try {
        enc.encodeHeader({
          version: 3,
          flags: HeaderFlagNoChecksum,
          pageSize,
          commit: finalCommit,
          minTXID: 1n,
          maxTXID,
          timestamp: BigInt(Date.now()),
          preApplyChecksum: 0n,
          walOffset: BigInt(WALHeaderSize),
          walSize: BigInt(Math.max(0, walBuf.length - WALHeaderSize)),
          walSalt1: salt1,
          walSalt2: salt2,
          nodeID: 0n,
        });
        await this.writePagesFromDB(enc, pageSize, finalCommit, lockPg, pageMap, walBuf, dbHandle);
        enc.close();
        await sink.flush();
        await fh.close();
      } catch (err) {
        await fh.close().catch(() => {});
        await rm(tmp, { force: true }).catch(() => {});
        throw err;
      }

      await rename(tmp, filename);

      const size = (await stat(filename)).size;
      return { level: 9, minTXID: 1n, maxTXID, size, createdAt: Date.now() };
    } finally {
      if (dbHandle !== null) await dbHandle.close();
      this.commitRead();
    }
  }

  /** Reads a single page, preferring the WAL over the database file. */
  private async readPage(
    pgno: number,
    pageSize: number,
    pageMap: Map<number, number>,
    walBuf: Uint8Array,
    dbHandle: FileHandle,
  ): Promise<Uint8Array> {
    const off = pageMap.get(pgno);
    if (off !== undefined) {
      const start = off + WALFrameHeaderSize;
      return new Uint8Array(walBuf.subarray(start, start + pageSize));
    }
    const page = new Uint8Array(pageSize);
    await readExact(dbHandle, page, (pgno - 1) * pageSize);
    return page;
  }

  /** Reads the WAL file bytes, returning empty if it does not exist. */
  private async readWalOrEmpty(): Promise<Uint8Array> {
    try {
      return new Uint8Array(await readFile(this.walPath()));
    } catch {
      return new Uint8Array(0);
    }
  }

  /** Decodes the header of the max-TXID local L0 file (or null if none). */
  private async prevSyncInfo(): Promise<{
    walOffset: number;
    walSize: number;
    salt1: number;
    salt2: number;
    commit: number;
  } | null> {
    const max = await this.maxFile();
    // After a restore the WAL position of the local files, if any, does not
    // describe the restored database: the next sync starts from a snapshot.
    if (max === null || max.maxTXID < (await this.baseTXID())) return null;
    try {
      // Only the 100-byte header is needed; do not read the whole file.
      const fh = await open(this.ltxPath(max.level, max.minTXID, max.maxTXID), 'r');
      try {
        const buf = new Uint8Array(HeaderSize);
        await readExact(fh, buf, 0);
        const hdr = new Decoder(buf);
        hdr.decodeHeader();
        const h: Header = hdr.Header();
        return {
          walOffset: Number(h.walOffset),
          walSize: Number(h.walSize),
          salt1: h.walSalt1,
          salt2: h.walSalt2,
          commit: h.commit,
        };
      } finally {
        await fh.close();
      }
    } catch {
      return null;
    }
  }

  /** Close releases the internal read-only connection. */
  close(): void {
    if (this.conn !== null) {
      this.conn.close();
      this.conn = null;
    }
  }
}

/** Creates a WALReader positioned at `offset`, or null if it cannot resume. */
function makeResumeReader(walBuf: Uint8Array, offset: number, salt1: number, salt2: number): WALReader | null {
  if (walBuf.length === 0) return null;
  if (offset <= WALHeaderSize) {
    // Starting fresh (or before any frames): read from the header.
    try {
      return new WALReader(walBuf);
    } catch {
      return null;
    }
  }
  try {
    // A WAL restarted since the last sync has new salts in its header, even if
    // frames of the old WAL are still on disk past the new ones.
    const hdr = new WALReader(walBuf);
    if (hdr.salt1() !== salt1 || hdr.salt2() !== salt2) return null;
    return WALReader.withOffset(walBuf, offset, salt1, salt2);
  } catch (err) {
    if (err instanceof PrevFrameMismatchError || err instanceof WALEOFError) return null;
    throw err;
  }
}

/**
 * Creates a WALReader from the start of a WAL restarted by a checkpoint, or
 * null if the WAL is not the one that directly followed it (salt-1 is
 * incremented by one on every restart).
 */
function makeNextReader(walBuf: Uint8Array, nextSalt1: number): WALReader | null {
  try {
    const r = new WALReader(walBuf);
    return r.salt1() === nextSalt1 ? r : null;
  } catch (err) {
    if (err instanceof WALEOFError) return null;
    throw err;
  }
}

/** Reads exactly `buf.length` bytes into `buf` from `handle` at `position`. */
async function readExact(
  handle: FileHandle,
  buf: Uint8Array,
  position: number,
): Promise<void> {
  let offset = 0;
  while (offset < buf.length) {
    const { bytesRead } = await handle.read(buf, offset, buf.length - offset, position + offset);
    if (bytesRead === 0) throw new DatabaseError('unexpected end of file while reading a page');
    offset += bytesRead;
  }
}

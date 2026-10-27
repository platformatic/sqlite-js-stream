/**
 * PostgreSQL replica client — replicates LTX files to a PostgreSQL database.
 *
 * Storage mirrors the S3/file object layout, scoped by a `prefix` column:
 *   `prefix` + `level` + `min_txid` + `max_txid` uniquely identifies an LTX file.
 *
 * Files are stored as one row in `ltx_files` plus one row per `ltx_chunks` chunk
 * (1 MiB by default). A file row and its chunks are written in a single
 * transaction, so a reader never observes a partially-written file. The primary
 * key makes a TXID range writable only once (a concurrent duplicate upload
 * fails with a unique-constraint error).
 *
 * The client also supports *fenced writes*: when a {@link PgReplicaClientOptions.generation}
 * is set, every upload locks the `ltx_generations` row (`FOR SHARE`) and rejects
 * the write with {@link ErrGenerationSuperseded} if a newer generation is
 * already registered. This lets an application elect the writer so a paused
 * former writer that wakes up after losing its lease can never append to the
 * replica.
 */

import { Readable } from 'node:stream';

import postgres, { type ISql, type Sql } from 'postgres';

import {
  ErrGenerationSuperseded,
  HeaderSize,
  InvalidArgumentError,
  LTXError,
  formatFilename,
  noopLogger,
  unmarshalHeader,
  type FileInfo,
  type Logger,
  type LTXFilesOptions,
  type OpenLTXFileOptions,
  type ReplicaClient,
  type WriteLTXFileOptions,
} from '@platformatic/sqlite-js-stream';

/** The strategy identifier reported by {@link PgReplicaClient.type}. */
export const ReplicaClientType = 'pg';

/** Default chunk size (1 MiB) for splitting LTX files into `ltx_chunks` rows. */
export const DefaultChunkSize = 1024 * 1024;

/** Configuration for {@link PgReplicaClient}. */
export interface PgReplicaClientOptions {
  /**
   * PostgreSQL connection URL (e.g. `postgres://user:pass@host:5432/db`).
   * Required unless a {@link sql} client is injected.
   */
  url?: string;
  /** Injected `postgres()` client. Set this (instead of {@link url}) in tests. */
  sql?: Sql;
  /** Replica prefix (row scoping). Trailing slash is normalized away. */
  prefix?: string;
  /** Schema holding the LTX tables. Defaults to `public`. */
  schema?: string;
  /** Base table-name prefix. Defaults to `ltx` (`ltx_files`, `ltx_chunks`, …). */
  tablePrefix?: string;
  /** Chunk size in bytes. Defaults to {@link DefaultChunkSize}. */
  chunkSize?: number;
  /** Create the LTX tables on {@link PgReplicaClient.init}. Defaults to `true`. */
  createTables?: boolean;
  /** Fenced-write generation, or `null` to disable fencing. */
  generation?: bigint | null;
  /** Max pool connections, passed through to `postgres()`. */
  max?: number;
}

/** Normalized base prefix (no leading/trailing slash, `''` when empty). */
function normalizePrefix(prefix: string): string {
  return prefix.replace(/^\/+/, '').replace(/\/+$/, '');
}

/** Concatenate byte chunks into a single Uint8Array. */
function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}

/** Normalize a body chunk (`Uint8Array | string`) to bytes. */
function toBytes(chunk: Uint8Array | string): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : Uint8Array.from(chunk);
}

/**
 * Postgres.js serializes `bigint` values as strings, and its TypeScript types
 * exclude `bigint` from interpolated query parameters. Passing a decimal string
 * is equivalent and lets PostgreSQL coerce the value to a BIGINT column.
 */
function txid(v: bigint): string {
  return v.toString();
}

/** Shape of a row from `ltx_chunks`. */
interface ChunkRow {
  seq: number;
  data: Uint8Array;
}

/** Table-name identifier must be a plain SQL identifier (prevents injection). */
const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** PgReplicaClient writes LTX files to PostgreSQL. */
export class PgReplicaClient implements ReplicaClient {
  readonly prefix: string;
  readonly schema: string;
  readonly tablePrefix: string;
  readonly chunkSize: number;
  private readonly createTables: boolean;
  private readonly sql: Sql;
  private readonly ownSql: boolean;
  private logger: Logger = noopLogger;
  private generation: bigint | null = null;
  private initialized = false;

  constructor(opts: PgReplicaClientOptions = {}) {
    if (!opts.url && !opts.sql) {
      throw new InvalidArgumentError(
        'pg replica client: either url or sql is required',
      );
    }
    this.prefix = normalizePrefix(opts.prefix ?? '');
    this.schema = opts.schema ?? 'public';
    this.tablePrefix = opts.tablePrefix ?? 'ltx';
    this.chunkSize = opts.chunkSize ?? DefaultChunkSize;
    this.createTables = opts.createTables ?? true;
    this.generation = opts.generation ?? null;
    this.ownSql = opts.sql === undefined;
    this.sql = opts.sql ?? postgres(opts.url!, { max: opts.max });
    if (this.chunkSize < 1) {
      throw new InvalidArgumentError('pg replica client: chunkSize must be >= 1');
    }
    for (const ident of [this.schema, this.tablePrefix]) {
      if (!IDENT_RE.test(ident)) {
        throw new InvalidArgumentError(
          `pg replica client: invalid identifier "${ident}" (schema/tablePrefix must be plain SQL identifiers)`,
        );
      }
    }
  }

  type(): string {
    return ReplicaClientType;
  }

  /** Ends the owned `postgres()` connection (no-op for an injected one). */
  close(): Promise<void> {
    if (this.ownSql) return this.sql.end({ timeout: 1 });
    return Promise.resolve();
  }

  /** Returns the schema-qualified table name reference for a base table suffix. */
  private table(name: string): string {
    return `${this.schema}.${this.tablePrefix}_${name}`;
  }

  private filesTable(): string {
    return this.table('files');
  }

  private chunksTable(): string {
    return this.table('chunks');
  }

  private generationsTable(): string {
    return this.table('generations');
  }

  private leasesTable(): string {
    return this.table('leases');
  }

  /** Logical LTX path used for {@link LTXError} messages. */
  private filePathKey(level: number, minTXID: bigint, maxTXID: bigint): string {
    return `${this.chunksTable()}/${level}/${formatFilename(minTXID, maxTXID)}`;
  }

  /** Creates the LTX tables (idempotent). */
  private async createTablesNow(): Promise<void> {
    await this.sql`
      CREATE TABLE IF NOT EXISTS ${this.sql(this.filesTable())} (
        prefix TEXT NOT NULL,
        level SMALLINT NOT NULL,
        min_txid BIGINT NOT NULL,
        max_txid BIGINT NOT NULL,
        size BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (prefix, level, min_txid, max_txid)
      )
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS ${this.sql(this.chunksTable())} (
        prefix TEXT NOT NULL,
        level SMALLINT NOT NULL,
        min_txid BIGINT NOT NULL,
        max_txid BIGINT NOT NULL,
        seq INT NOT NULL,
        data BYTEA NOT NULL,
        PRIMARY KEY (prefix, level, min_txid, max_txid, seq)
      )
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS ${this.sql(this.generationsTable())} (
        prefix TEXT PRIMARY KEY,
        generation BIGINT NOT NULL
      )
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS ${this.sql(this.leasesTable())} (
        prefix TEXT PRIMARY KEY,
        generation BIGINT NOT NULL,
        owner TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        version BIGINT NOT NULL
      )
    `;
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    await this.sql`SELECT 1`;
    if (this.createTables) {
      await this.createTablesNow();
    }
    this.initialized = true;
  }

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /** Current fenced-write generation, or `null` when fencing is disabled. */
  getGeneration(): bigint | null {
    return this.generation;
  }

  /**
   * Raises the stored replication generation to `generation` and fails with
   * {@link ErrGenerationSuperseded} if a higher one is already registered.
   * After a successful call, uploads from an older generation are rejected.
   */
  async registerGeneration(generation: bigint): Promise<void> {
    await this.init();
    const res = await this.sql`
      INSERT INTO ${this.sql(this.generationsTable())} (prefix, generation)
      VALUES (${this.prefix}, ${txid(generation)})
      ON CONFLICT (prefix) DO UPDATE SET generation = EXCLUDED.generation
      WHERE ${this.sql(this.generationsTable())}.generation <= EXCLUDED.generation
      RETURNING generation
    `;
    if (res.length === 0) {
      const cur = await this.sql`
        SELECT generation FROM ${this.sql(this.generationsTable())} WHERE prefix = ${this.prefix}
      `;
      const stored = cur.length > 0 ? BigInt(cur[0].generation) : 0n;
      throw new ErrGenerationSuperseded(stored);
    }
    this.generation = generation;
    this.logger.debug('registered generation', { generation });
  }

  /**
   * Inside a write transaction, locks the generation row (`FOR SHARE`) and
   * throws {@link ErrGenerationSuperseded} if a newer generation is registered.
   */
  private async assertFenced(sql: ISql): Promise<void> {
    if (this.generation === null) return;
    const rows = await sql`
      SELECT generation FROM ${this.sql(this.generationsTable())}
      WHERE prefix = ${this.prefix} FOR SHARE
    `;
    if (rows.length > 0) {
      const stored = BigInt(rows[0].generation);
      if (stored > this.generation) {
        throw new ErrGenerationSuperseded(stored);
      }
    }
  }

  async *ltxFiles(opts: LTXFilesOptions): AsyncIterable<FileInfo> {
    await this.init();
    const rows = await this.sql`
      SELECT min_txid, max_txid, size, created_at
      FROM ${this.sql(this.filesTable())}
      WHERE prefix = ${this.prefix} AND level = ${opts.level} AND min_txid >= ${txid(opts.seek)}
      ORDER BY min_txid ASC, max_txid ASC
    `;
    for (const row of rows) {
      yield {
        level: opts.level,
        minTXID: BigInt(row.min_txid),
        maxTXID: BigInt(row.max_txid),
        size: Number(row.size),
        createdAt: new Date(row.created_at).getTime(),
      };
    }
  }

  async openLtxFile(opts: OpenLTXFileOptions): Promise<Readable> {
    await this.init();
    const { level, minTXID, maxTXID, offset, size } = opts;
    const path = this.filePathKey(level, minTXID, maxTXID);

    let fileRow;
    try {
      fileRow = await this.sql`
        SELECT size FROM ${this.sql(this.filesTable())}
        WHERE prefix = ${this.prefix} AND level = ${level}
          AND min_txid = ${txid(minTXID)} AND max_txid = ${txid(maxTXID)}
      `;
    } catch (err) {
      throw new LTXError('open', path, level, minTXID, maxTXID, err);
    }
    if (fileRow.length === 0) {
      throw new LTXError('open', path, level, minTXID, maxTXID, new Error('not found'));
    }

    const fileSize = Number(fileRow[0].size);
    if (offset >= fileSize) return Readable.from([]);
    const end = size > 0 ? Math.min(offset + size - 1, fileSize - 1) : fileSize - 1;
    if (end < offset) return Readable.from([]);

    const firstSeq = Math.floor(offset / this.chunkSize);
    const lastSeq = Math.floor(end / this.chunkSize);

    // Stream the chunk rows back lazily via a server-side cursor so a large
    // snapshot is never buffered whole in memory (matching the file/S3
    // backends). Breaking out of the stream early closes the portal and
    // releases the connection.
    let cursor: AsyncIterable<ChunkRow[]>;
    try {
      cursor = this.sql`
        SELECT seq, data FROM ${this.sql(this.chunksTable())}
        WHERE prefix = ${this.prefix} AND level = ${level}
          AND min_txid = ${txid(minTXID)} AND max_txid = ${txid(maxTXID)}
          AND seq BETWEEN ${firstSeq} AND ${lastSeq}
        ORDER BY seq ASC
      `.cursor(16) as unknown as AsyncIterable<ChunkRow[]>;
    } catch (err) {
      throw new LTXError('open', path, level, minTXID, maxTXID, err);
    }

    return Readable.from(this.iterateChunkCursor(cursor, offset, end));
  }

  /** Streams the requested byte range from a chunk-row cursor. */
  private async *iterateChunkCursor(
    cursor: AsyncIterable<ChunkRow[]>,
    offset: number,
    end: number,
  ): AsyncIterable<Uint8Array> {
    for await (const rows of cursor) {
      for (const row of rows) {
        const data = row.data;
        const chunkStart = Number(row.seq) * this.chunkSize;
        const chunkEnd = chunkStart + data.length - 1;
        if (chunkEnd < offset) continue;
        if (chunkStart > end) return;
        const s = Math.max(offset, chunkStart) - chunkStart;
        const e = Math.min(end, chunkEnd) - chunkStart;
        yield data.subarray(s, e + 1);
      }
    }
  }

  async writeLtxFile(
    opts: WriteLTXFileOptions,
    body: AsyncIterable<Uint8Array | string>,
  ): Promise<FileInfo> {
    await this.init();
    const { level, minTXID, maxTXID } = opts;
    const path = this.filePathKey(level, minTXID, maxTXID);

    try {
      let info: { size: number; createdAt: number } | null = null;
      await this.sql.begin(async (tx) => {
        await this.assertFenced(tx);

        // Capture the LTX header (timestamp) from the leading bytes, and split
        // the body into fixed-size chunks. Chunks are inserted as they arrive so
        // memory stays bounded by ~chunkSize, and a reader never sees a partial
        // file because everything commits atomically.
        let headerBytes: Uint8Array | null = null;
        const pending: Uint8Array[] = [];
        let pendingLen = 0;
        let seq = 0;
        let size = 0;

        const emitFullChunk = async (): Promise<void> => {
          const buf = new Uint8Array(this.chunkSize);
          let copied = 0;
          while (copied < this.chunkSize) {
            const head = pending[0];
            const take = Math.min(head.length, this.chunkSize - copied);
            buf.set(head.subarray(0, take), copied);
            copied += take;
            if (take === head.length) {
              pending.shift();
            } else {
              pending[0] = head.subarray(take);
            }
          }
          pendingLen -= this.chunkSize;
          await tx`
            INSERT INTO ${this.sql(this.chunksTable())}
              (prefix, level, min_txid, max_txid, seq, data)
            VALUES (${this.prefix}, ${level}, ${txid(minTXID)}, ${txid(maxTXID)}, ${seq}, ${buf})
          `;
          seq++;
        };

        for await (const chunk of body) {
          const bytes = toBytes(chunk);
          if (bytes.length === 0) continue;

          if (headerBytes === null && pendingLen + bytes.length >= HeaderSize) {
            const collected = pending.length > 0 ? concatBytes([...pending, bytes]) : bytes;
            headerBytes = collected.subarray(0, HeaderSize);
          }

          size += bytes.length;
          pending.push(bytes);
          pendingLen += bytes.length;

          while (pendingLen >= this.chunkSize) {
            await emitFullChunk();
          }
        }

        if (headerBytes === null) {
          throw new InvalidArgumentError('write ltx file: input too short to contain a header');
        }
        const timestamp = Number(unmarshalHeader(headerBytes).timestamp);

        // Remaining tail chunk.
        if (pendingLen > 0) {
          const buf = pending.length === 1 ? pending[0] : concatBytes(pending);
          await tx`
            INSERT INTO ${this.sql(this.chunksTable())}
              (prefix, level, min_txid, max_txid, seq, data)
            VALUES (${this.prefix}, ${level}, ${txid(minTXID)}, ${txid(maxTXID)}, ${seq}, ${buf})
          `;
          seq++;
        }

        await tx`
          INSERT INTO ${this.sql(this.filesTable())}
            (prefix, level, min_txid, max_txid, size, created_at)
          VALUES (${this.prefix}, ${level}, ${txid(minTXID)}, ${txid(maxTXID)}, ${size}, ${new Date(timestamp)})
        `;

        info = { size, createdAt: timestamp };
      });

      return {
        level,
        minTXID,
        maxTXID,
        size: info!.size,
        createdAt: info!.createdAt,
      };
    } catch (err) {
      if (err instanceof ErrGenerationSuperseded) throw err;
      throw new LTXError('write', path, level, minTXID, maxTXID, err);
    }
  }

  async deleteLtxFiles(infos: FileInfo[]): Promise<void> {
    await this.init();
    for (const info of infos) {
      await this.sql.begin(async (tx) => {
        await tx`
          DELETE FROM ${this.sql(this.chunksTable())}
          WHERE prefix = ${this.prefix} AND level = ${info.level}
            AND min_txid = ${txid(info.minTXID)} AND max_txid = ${txid(info.maxTXID)}
        `;
        await tx`
          DELETE FROM ${this.sql(this.filesTable())}
          WHERE prefix = ${this.prefix} AND level = ${info.level}
            AND min_txid = ${txid(info.minTXID)} AND max_txid = ${txid(info.maxTXID)}
        `;
      });
    }
  }

  async deleteAll(): Promise<void> {
    await this.init();
    await this.sql.begin(async (tx) => {
      await tx`DELETE FROM ${this.sql(this.chunksTable())} WHERE prefix = ${this.prefix}`;
      await tx`DELETE FROM ${this.sql(this.filesTable())} WHERE prefix = ${this.prefix}`;
    });
  }
}

/**
 * Builds a PgReplicaClient from parsed replica URL parts. Connection details come
 * from `host`/`urlPath`/`userinfo`, while the replica `prefix` (and optional
 * `schema`) are read from the query string.
 *
 * E.g. `postgres://user:pass@host:5432/mydb?prefix=app/db1&schema=replica`.
 */
export function newReplicaClientFromURL(
  scheme: string,
  host: string,
  urlPath: string,
  query: URLSearchParams | null,
  userinfo: string | null,
): ReplicaClient {
  // Split `host` into hostname and port, handling bracketed IPv6 hosts.
  let hostname: string;
  let port: number | undefined;
  if (host.startsWith('[')) {
    const close = host.indexOf(']');
    hostname = host.slice(0, close + 1);
    const rest = host.slice(close + 1);
    port = rest.startsWith(':') ? Number(rest.slice(1)) : undefined;
  } else {
    const idx = host.lastIndexOf(':');
    if (idx >= 0) {
      hostname = host.slice(0, idx);
      const p = host.slice(idx + 1);
      if (p.length > 0 && /^\d+$/.test(p)) port = Number(p);
      else hostname = host;
    } else {
      hostname = host;
    }
  }
  const [username, password] = userinfo ? userinfo.split(':') : ['', ''];

  // Rebuild a connection URL, stripping the replica-specific query params the
  // driver would not understand (prefix / schema).
  const params = new URLSearchParams();
  for (const [k, v] of query ?? []) {
    if (k === 'prefix' || k === 'schema') continue;
    params.set(k, v);
  }
  const qs = params.toString();
  const user = username || password
    ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`
    : '';
  let url = `${scheme}://${user}${hostname}`;
  if (port !== undefined) url += `:${port}`;
  url += urlPath;
  if (qs) url += `?${qs}`;

  const prefix = query?.get('prefix') ?? '';
  const schema = query?.get('schema') ?? undefined;

  return new PgReplicaClient({
    url,
    prefix,
    schema,
  });
}

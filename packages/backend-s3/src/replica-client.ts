/**
 * S3 replica client — replicates LTX files to an Amazon S3 bucket.
 *
 * Object layout mirrors the file backend:
 *   `{prefix}ltx/{level}/{minTXID}-{maxTXID}.ltx`
 *
 * The implementation talks to S3 through the AWS SDK v3 {@link S3Client}. A
 * minimal `send`-shaped client can be injected (e.g. an in-memory fake in
 * tests) so the upload strategy is testable without network access.
 *
 * Creation timestamps are preserved on upload as the `ltx-timestamp` object
 * metadata; listing falls back to the object's `LastModified`.
 *
 * The client also supports *fenced writes* (see {@link S3ReplicaClient.registerGeneration}).
 * S3 cannot check one object and write another in a single request, so fencing
 * is a protocol built from conditional writes:
 *
 * - `{prefix}/generation.json` holds the current generation and a list of
 *   *fences*. A fence `(generation, txid)` records the latest TXID of the
 *   replica right after `generation` was registered. Registering is a
 *   conditional write (`If-Match` / `If-None-Match: *`) that only raises the
 *   generation; the fence is then added with another conditional write.
 * - LTX files of a fenced client are written with `If-None-Match: *`, so a TXID
 *   range is written once per level, and carry their generation in the
 *   `ltx-generation` metadata. A file with no `ltx-generation` is generation 0.
 * - Listings ignore a file whose generation is lower than a fence's generation
 *   and whose `maxTXID` is above that fence's TXID: it was written by a
 *   superseded writer after the new one read the replica.
 * - After each upload, a fenced client re-reads the generation object and fails
 *   with {@link ErrGenerationSuperseded} if a higher generation is registered.
 *   An upload that resolves was therefore written while its generation was the
 *   latest, and every later generation sees it.
 */

import { Readable } from 'node:stream';

import {
  S3Client,
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  UploadPartCommand,
} from '@aws-sdk/client-s3';

import {
  ErrGenerationSuperseded,
  HeaderSize,
  LTXError,
  SnapshotLevel,
  formatFilename,
  noopLogger,
  parseFilename,
  unmarshalHeader,
  type FileInfo,
  type Logger,
  type LTXFilesOptions,
  type OpenLTXFileOptions,
  type ReplicaClient,
  type WriteLTXFileOptions,
} from '@platformatic/sqlite-js-stream';

import { isConditionalConflict, isNotFound, isPreconditionFailed, readBody } from './util.ts';

/** The strategy identifier reported by {@link S3ReplicaClient.type}. */
export const ReplicaClientType = 's3';

/**
 * Minimal shape of a client used to send commands. The AWS SDK v3 `S3Client`
 * satisfies this; tests inject an in-memory fake instead of hitting the network.
 */
export interface S3ClientLike {
  send(command: any): Promise<any>;
}

/** Configuration for {@link S3ReplicaClient}. */
export interface S3ReplicaClientOptions {
  /** S3 bucket name. Required. */
  bucket: string;
  /** Object key prefix (e.g. `backup`). Trailing slash is added as needed. */
  prefix?: string;
  /** AWS region. Defaults to the AWS SDK region resolution chain. */
  region?: string;
  /** Custom endpoint URL (e.g. for MinIO / LocalStack). */
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  /** Inject a preconfigured client. Mostly for tests. */
  client?: S3ClientLike;
  /**
   * Fenced-write generation, or `null` (the default) to disable fencing. Set
   * by {@link S3ReplicaClient.registerGeneration}; when set, uploads and
   * deletions fail with {@link ErrGenerationSuperseded} once a higher
   * generation is registered.
   */
  generation?: bigint | null;
}

/** Name of the generation object, relative to the replica prefix. */
export const GenerationObjectName = 'generation.json';

/** Object metadata key carrying the generation of a fenced LTX upload. */
export const GenerationMetadataKey = 'ltx-generation';

/**
 * A fence: `generation` was registered when the latest TXID of the replica was
 * `txid`. Files of lower generations with a `maxTXID` above `txid` are ignored.
 */
export interface GenerationFence {
  generation: bigint;
  txid: bigint;
}

/** Contents of the generation object. */
export interface GenerationState {
  /** The highest registered generation. */
  generation: bigint;
  /** Fences, in registration order (one per generation that completed registration). */
  fences: GenerationFence[];
}

/**
 * Returns true if a file of `generation` ending at `maxTXID` is part of the
 * replica: no fence of a higher generation was recorded below `maxTXID`.
 */
export function isFenced(fences: readonly GenerationFence[], generation: bigint, maxTXID: bigint): boolean {
  for (const f of fences) {
    if (f.generation > generation && maxTXID > f.txid) return false;
  }
  return true;
}

/** Retries of a conditional write that keeps conflicting with concurrent ones. */
const MAX_CONDITIONAL_ATTEMPTS = 10;

/** HeadObject requests in flight while resolving the generation of listed files. */
const HEAD_CONCURRENCY = 16;

/** Cached generations of listed objects (by key and ETag). */
const GENERATION_CACHE_SIZE = 10_000;

/** Parses the `ltx-generation` metadata (0 when absent or invalid). */
function metadataGeneration(metadata: Record<string, string> | undefined): bigint {
  const v = metadata?.[GenerationMetadataKey];
  if (v === undefined || !/^\d+$/.test(v)) return 0n;
  return BigInt(v);
}

/** Serializes a generation state (bigints as decimal strings). */
function encodeGenerationState(state: GenerationState): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({
      generation: state.generation.toString(),
      fences: state.fences.map((f) => ({ generation: f.generation.toString(), txid: f.txid.toString() })),
    }),
  );
}

/** Parses a generation object body. */
function decodeGenerationState(data: Uint8Array): GenerationState {
  const parsed = JSON.parse(new TextDecoder().decode(data)) as {
    generation?: string;
    fences?: Array<{ generation?: string; txid?: string }>;
  };
  return {
    generation: BigInt(parsed.generation ?? '0'),
    fences: (parsed.fences ?? []).map((f) => ({
      generation: BigInt(f.generation ?? '0'),
      txid: BigInt(f.txid ?? '0'),
    })),
  };
}

/** Runs `fn` over `items` with at most `limit` calls in flight, preserving order. */
async function mapLimit<T, R>(items: readonly T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

/** Conditional headers of a write. */
type WriteCondition = { IfNoneMatch: '*' } | { IfMatch: string } | Record<string, never>;

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

/** S3 requires every multipart part (except the last) to be at least 5 MiB. */
const MULTIPART_MIN_PART_SIZE = 5 * 1024 * 1024;

/** S3ReplicaClient writes LTX files to an Amazon S3 bucket. */
export class S3ReplicaClient implements ReplicaClient {
  readonly bucket: string;
  readonly prefix: string;
  private client: S3ClientLike;
  private logger: Logger = noopLogger;
  private generation: bigint | null;
  /** The higher generation that superseded this client, once observed. */
  private supersededBy: bigint | null = null;
  /** Generation of listed objects, keyed by `key` + ETag (objects are immutable). */
  private readonly objectGenerations = new Map<string, bigint>();

  constructor(opts: S3ReplicaClientOptions) {
    if (!opts.bucket) {
      throw new Error('S3 replica client: bucket is required');
    }
    this.bucket = opts.bucket;
    this.prefix = normalizePrefix(opts.prefix ?? '');
    this.generation = opts.generation ?? null;
    this.client =
      opts.client ??
      new S3Client({
        region: opts.region,
        endpoint: opts.endpoint,
        credentials: opts.accessKeyId
          ? {
              accessKeyId: opts.accessKeyId,
              secretAccessKey: opts.secretAccessKey ?? '',
              sessionToken: opts.sessionToken,
            }
          : undefined,
      });
  }

  type(): string {
    return ReplicaClientType;
  }

  /** Best-effort connectivity check; real I/O errors surface on first use. */
  async init(): Promise<void> {
    try {
      await this.client.send(new HeadBucketCommand({ Bucket: this.bucket }));
    } catch (err) {
      this.logger.debug('s3 head bucket failed', err);
    }
  }

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /** Returns the object key prefix (empty string if none). */
  prefixKey(): string {
    return this.prefix;
  }

  /** Returns `{prefix}ltx/{level}/`. */
  private levelPrefix(level: number): string {
    return `${this.prefix ? `${this.prefix}/` : ''}ltx/${level}/`;
  }

  /** Returns the object key for a level and TXID range. */
  private fileKey(level: number, minTXID: bigint, maxTXID: bigint): string {
    return `${this.levelPrefix(level)}${formatFilename(minTXID, maxTXID)}`;
  }

  /** Returns the key of the generation object. */
  private generationKey(): string {
    return `${this.prefix ? `${this.prefix}/` : ''}${GenerationObjectName}`;
  }

  /** Current fenced-write generation, or `null` when fencing is disabled. */
  getGeneration(): bigint | null {
    return this.generation;
  }

  /** Reads the generation object, or returns `null` if fencing was never used. */
  async getGenerationState(): Promise<GenerationState | null> {
    return (await this.readGenerationObject())?.state ?? null;
  }

  private async readGenerationObject(): Promise<{ state: GenerationState; etag: string } | null> {
    let res;
    try {
      res = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: this.generationKey() }));
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
    const state = decodeGenerationState(await readBody(res.Body));
    return { state, etag: String(res.ETag ?? '') };
  }

  /**
   * Writes the generation object if it was not changed since it was read with
   * `etag` (`null`: if it does not exist). Returns false on a lost race.
   */
  private async writeGenerationObject(state: GenerationState, etag: string | null): Promise<boolean> {
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.generationKey(),
          Body: encodeGenerationState(state),
          ContentType: 'application/json',
          ...(etag === null ? { IfNoneMatch: '*' } : { IfMatch: etag }),
        }),
      );
      return true;
    } catch (err) {
      if (isPreconditionFailed(err) || isConditionalConflict(err)) return false;
      throw err;
    }
  }

  /** Records that this client was superseded by `generation` and returns the error. */
  private superseded(generation: bigint): ErrGenerationSuperseded {
    this.supersededBy = generation;
    return new ErrGenerationSuperseded(generation);
  }

  /**
   * Fails with {@link ErrGenerationSuperseded} if a generation higher than this
   * client's is registered. A no-op when fencing is disabled.
   */
  private async assertGeneration(): Promise<void> {
    if (this.generation === null) return;
    if (this.supersededBy !== null) throw new ErrGenerationSuperseded(this.supersededBy);
    const cur = await this.readGenerationObject();
    if (cur !== null && cur.state.generation > this.generation) {
      throw this.superseded(cur.state.generation);
    }
  }

  /**
   * Raises the replica's generation to `generation`, failing with
   * {@link ErrGenerationSuperseded} if a higher one is already registered, and
   * records a fence at the replica's latest TXID as read after the
   * registration. From then on, files of lower generations above that TXID
   * are ignored by every listing, and uploads and deletions of lower
   * generations fail once they observe the new generation.
   *
   * Call it before restoring: the restore then sees every file that a former
   * writer can still make part of the replica. Registering the current
   * generation again is a no-op that keeps its fence.
   */
  async registerGeneration(generation: bigint): Promise<void> {
    // 1. Raise the generation (compare-and-swap on the generation object).
    for (let attempt = 0; ; attempt++) {
      if (attempt >= MAX_CONDITIONAL_ATTEMPTS) {
        throw new Error('S3 replica client: too many conflicts registering the generation');
      }
      const cur = await this.readGenerationObject();
      if (cur !== null && cur.state.generation > generation) {
        throw new ErrGenerationSuperseded(cur.state.generation);
      }
      if (cur !== null && cur.state.generation === generation) break;
      const next = { generation, fences: cur?.state.fences ?? [] };
      if (await this.writeGenerationObject(next, cur?.etag ?? null)) break;
    }
    this.generation = generation;
    this.supersededBy = null;

    // 2. Read the latest TXID of the replica. Files of lower generations that
    //    land from now on and end above it are not part of the replica.
    let txid = 0n;
    for (let level = 0; level <= SnapshotLevel; level++) {
      for await (const info of this.ltxFiles({ level, seek: 0n, useMetadata: false })) {
        if (info.maxTXID > txid) txid = info.maxTXID;
      }
    }

    // 3. Record the fence, unless a higher generation registered meanwhile.
    for (let attempt = 0; ; attempt++) {
      if (attempt >= MAX_CONDITIONAL_ATTEMPTS) {
        throw new Error('S3 replica client: too many conflicts recording the generation fence');
      }
      const cur = await this.readGenerationObject();
      const stored = cur?.state.generation ?? generation;
      if (stored > generation) throw this.superseded(stored);
      const fences = cur?.state.fences ?? [];
      if (fences.some((f) => f.generation === generation)) break;
      const next = { generation, fences: [...fences, { generation, txid }] };
      if (await this.writeGenerationObject(next, cur?.etag ?? null)) break;
    }
    this.logger.debug('registered generation', { generation, txid });
  }

  /** Returns the generation of an object (from `ltx-generation`), or null if it is gone. */
  private async objectGeneration(key: string, etag: string | undefined): Promise<bigint | null> {
    const cacheKey = etag ? `${key}\n${etag}` : null;
    if (cacheKey !== null) {
      const cached = this.objectGenerations.get(cacheKey);
      if (cached !== undefined) return cached;
    }
    let head;
    try {
      head = await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
    const generation = metadataGeneration(head.Metadata);
    this.rememberGeneration(key, head.ETag ?? etag, generation);
    return generation;
  }

  private rememberGeneration(key: string, etag: string | undefined, generation: bigint): void {
    if (!etag) return;
    if (this.objectGenerations.size >= GENERATION_CACHE_SIZE) this.objectGenerations.clear();
    this.objectGenerations.set(`${key}\n${etag}`, generation);
  }

  /**
   * Lists the LTX files of a level. When the replica has fences, files ending
   * above the lowest fence need their generation (a HeadObject request, cached
   * per key and ETag), and those written by a superseded writer are skipped.
   */
  async *ltxFiles(opts: LTXFilesOptions): AsyncIterable<FileInfo> {
    const prefix = this.levelPrefix(opts.level);
    const fences = (await this.readGenerationObject())?.state.fences ?? [];
    let lowestFence: bigint | null = null;
    for (const f of fences) {
      if (lowestFence === null || f.txid < lowestFence) lowestFence = f.txid;
    }

    let token: string | undefined;
    do {
      const res = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: prefix,
          ContinuationToken: token,
        }),
      );
      const page: Array<{ info: FileInfo; key: string; etag: string | undefined }> = [];
      for (const obj of res.Contents ?? []) {
        if (!obj.Key) continue;
        const name = obj.Key.slice(prefix.length);
        let minTXID: bigint;
        let maxTXID: bigint;
        try {
          ({ minTXID, maxTXID } = parseFilename(name));
        } catch {
          continue; // skip non-conforming keys
        }
        if (minTXID < opts.seek) continue;
        page.push({
          key: obj.Key,
          etag: obj.ETag,
          info: {
            level: opts.level,
            minTXID,
            maxTXID,
            size: obj.Size ?? 0,
            createdAt: (obj.LastModified ?? new Date(0)).getTime(),
          },
        });
      }

      let visible: boolean[] = page.map(() => true);
      if (lowestFence !== null) {
        const fence = lowestFence;
        visible = await mapLimit(page, HEAD_CONCURRENCY, async (p) => {
          if (p.info.maxTXID <= fence) return true;
          const generation = await this.objectGeneration(p.key, p.etag);
          return generation !== null && isFenced(fences, generation, p.info.maxTXID);
        });
      }
      for (let i = 0; i < page.length; i++) {
        if (visible[i]) yield page[i].info;
      }
      token = res.IsTruncated ? (res.NextContinuationToken ?? undefined) : undefined;
    } while (token);
  }

  async openLtxFile(opts: OpenLTXFileOptions): Promise<Readable> {
    const key = this.fileKey(opts.level, opts.minTXID, opts.maxTXID);

    // A zero offset/size is a full read; no HeadObject round-trip needed.
    if (opts.offset === 0 && opts.size === 0) {
      try {
        const res = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
        return res.Body as Readable;
      } catch (err) {
        throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
      }
    }

    // Need the object size to build a range and to handle offset >= size.
    let head;
    try {
      head = await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
    } catch (err) {
      throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
    }
    const len = Number(head.ContentLength ?? 0);
    if (opts.offset >= len) return Readable.from([]);

    const end = opts.size > 0 ? Math.min(opts.offset + opts.size - 1, len - 1) : len - 1;
    if (end < opts.offset) return Readable.from([]);

    try {
      const res = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Range: `bytes=${opts.offset}-${end}`,
        }),
      );
      return res.Body as Readable;
    } catch (err) {
      throw new LTXError('open', key, opts.level, opts.minTXID, opts.maxTXID, err);
    }
  }

  async writeLtxFile(opts: WriteLTXFileOptions, body: AsyncIterable<Uint8Array | string>): Promise<FileInfo> {
    const key = this.fileKey(opts.level, opts.minTXID, opts.maxTXID);
    const generation = this.generation;
    if (generation !== null && this.supersededBy !== null) {
      throw new ErrGenerationSuperseded(this.supersededBy);
    }

    // Capture the LTX header (timestamp) from the leading bytes, and stream the
    // rest. Small objects use a single PutObject; large objects (> 5 MiB) are
    // uploaded with the multipart API so the whole file is never buffered.
    let headerBytes: Uint8Array | null = null;
    let head: Uint8Array[] = [];
    let headLen = 0;

    let acc: Uint8Array[] = [];
    let accLen = 0;
    let size = 0;

    let uploadId: string | null = null;
    let completed = false;
    let partNumber = 0;
    const uploadedParts: Array<{ PartNumber: number; ETag: string }> = [];

    const uploadPart = async (buf: Uint8Array): Promise<void> => {
      partNumber++;
      const res = await this.client.send(
        new UploadPartCommand({
          Bucket: this.bucket,
          Key: key,
          UploadId: uploadId!,
          PartNumber: partNumber,
          Body: buf,
        }),
      );
      uploadedParts.push({ PartNumber: partNumber, ETag: res.ETag ?? `etag-${partNumber}` });
    };

    const metadataFor = (timestamp: number): Record<string, string> => {
      const metadata: Record<string, string> = { 'ltx-timestamp': String(timestamp) };
      if (generation !== null) metadata[GenerationMetadataKey] = generation.toString();
      return metadata;
    };

    try {
      for await (const chunk of body) {
        const bytes =
          typeof chunk === 'string' ? new TextEncoder().encode(chunk) : Uint8Array.from(chunk);
        if (bytes.length === 0) continue;
        size += bytes.length;

        if (headerBytes === null) {
          head.push(bytes);
          headLen += bytes.length;
          if (headLen >= HeaderSize) {
            headerBytes = concatBytes(head).subarray(0, HeaderSize);
            head = [];
            headLen = 0;
          }
        }

        acc.push(bytes);
        accLen += bytes.length;

        if (uploadId === null && accLen >= MULTIPART_MIN_PART_SIZE) {
          const res = await this.client.send(
            new CreateMultipartUploadCommand({
              Bucket: this.bucket,
              Key: key,
              Metadata: metadataFor(Number(unmarshalHeader(headerBytes!).timestamp)),
            }),
          );
          uploadId = res.UploadId;
          await uploadPart(concatBytes(acc));
          acc = [];
          accLen = 0;
        } else if (uploadId !== null && accLen >= MULTIPART_MIN_PART_SIZE) {
          await uploadPart(concatBytes(acc));
          acc = [];
          accLen = 0;
        }
      }

      if (headerBytes === null) {
        throw new Error('write ltx file: input too short to contain a header');
      }
      const timestamp = Number(unmarshalHeader(headerBytes).timestamp);

      // Multipart: upload any remaining bytes as the final part.
      if (uploadId !== null && accLen > 0) {
        await uploadPart(concatBytes(acc));
        acc = [];
        accLen = 0;
      }
      const buf = uploadId === null ? concatBytes(acc) : null;

      // Creates the object: a single PutObject for small objects, otherwise
      // completes the multipart upload.
      const commit = (condition: WriteCondition): Promise<{ ETag?: string }> => {
        if (buf !== null) {
          return this.client.send(
            new PutObjectCommand({
              Bucket: this.bucket,
              Key: key,
              Body: buf,
              ContentLength: size,
              Metadata: metadataFor(timestamp),
              ...condition,
            }),
          );
        }
        return this.client.send(
          new CompleteMultipartUploadCommand({
            Bucket: this.bucket,
            Key: key,
            UploadId: uploadId!,
            MultipartUpload: { Parts: uploadedParts },
            ...condition,
          }),
        );
      };

      const info: FileInfo = {
        level: opts.level,
        minTXID: opts.minTXID,
        maxTXID: opts.maxTXID,
        size,
        createdAt: timestamp,
      };

      if (generation === null) {
        await commit({});
        completed = true;
        return info;
      }

      // A multipart upload first looks for an existing object, so that the
      // completion is not expected to fail (S3 may not keep the upload open).
      const existing = await this.commitFenced(key, opts, generation, commit, uploadId !== null);
      completed = existing === null;

      // The upload is only acknowledged if this generation is still the latest
      // after it landed: any generation registered later then sees the file.
      await this.assertGeneration();
      return existing ?? info;
    } catch (err) {
      if (err instanceof ErrGenerationSuperseded) throw err;
      throw new LTXError('write', key, opts.level, opts.minTXID, opts.maxTXID, err);
    } finally {
      if (uploadId !== null && !completed) {
        await this.client
          .send(
            new AbortMultipartUploadCommand({
              Bucket: this.bucket,
              Key: key,
              UploadId: uploadId,
            }),
          )
          .catch(() => {});
      }
    }
  }

  /**
   * Creates a fenced LTX object with `If-None-Match: *`. If the key already
   * exists:
   *  - written by a higher generation: this client is superseded;
   *  - written by this generation (a retried upload), or by a lower one but
   *    still part of the replica: it holds the same transactions, so it is
   *    kept and its metadata returned;
   *  - otherwise it is a superseded writer's file, replaced with `If-Match`.
   * With `probe`, the key is checked with HeadObject before the first write.
   * Returns null when this call created the object.
   */
  private async commitFenced(
    key: string,
    opts: WriteLTXFileOptions,
    generation: bigint,
    commit: (condition: WriteCondition) => Promise<{ ETag?: string }>,
    probe: boolean,
  ): Promise<FileInfo | null> {
    let condition: WriteCondition = { IfNoneMatch: '*' };
    for (let attempt = 0; attempt < MAX_CONDITIONAL_ATTEMPTS; attempt++) {
      if (!probe) {
        try {
          const res = await commit(condition);
          this.rememberGeneration(key, res.ETag, generation);
          return null;
        } catch (err) {
          if (isConditionalConflict(err)) continue;
          if (!isPreconditionFailed(err)) throw err;
        }
      }
      probe = false;

      let head;
      try {
        head = await this.client.send(new HeadObjectCommand({ Bucket: this.bucket, Key: key }));
      } catch (err) {
        if (isNotFound(err)) {
          condition = { IfNoneMatch: '*' }; // deleted meanwhile
          continue;
        }
        throw err;
      }
      const existing = metadataGeneration(head.Metadata);
      if (existing > generation) throw this.superseded(existing);
      const fences = (await this.readGenerationObject())?.state.fences ?? [];
      if (existing === generation || isFenced(fences, existing, opts.maxTXID)) {
        const timestamp = Number(head.Metadata?.['ltx-timestamp']);
        return {
          level: opts.level,
          minTXID: opts.minTXID,
          maxTXID: opts.maxTXID,
          size: Number(head.ContentLength ?? 0),
          createdAt: Number.isFinite(timestamp)
            ? timestamp
            : (head.LastModified ?? new Date(0)).getTime(),
        };
      }
      this.logger.debug('replacing ltx file of a superseded writer', { key, generation: existing });
      condition = { IfMatch: String(head.ETag ?? '') };
    }
    throw new Error(`too many conflicting writes to ${key}`);
  }

  /**
   * Deletes LTX files. A fenced client first checks that its generation is
   * still the latest and fails with {@link ErrGenerationSuperseded} otherwise
   * (a deletion sent just before a new generation registers still applies).
   */
  async deleteLtxFiles(infos: FileInfo[]): Promise<void> {
    const objects = infos.map((info) => ({
      Key: this.fileKey(info.level, info.minTXID, info.maxTXID),
    }));
    for (let i = 0; i < objects.length; i += 1000) {
      await this.assertGeneration();
      await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: { Objects: objects.slice(i, i + 1000), Quiet: true },
        }),
      );
    }
  }

  /**
   * Deletes every object under the prefix except the generation object, so a
   * superseded writer stays fenced. A fenced client checks its generation
   * first, as {@link deleteLtxFiles} does.
   */
  async deleteAll(): Promise<void> {
    const prefix = this.prefix ? `${this.prefix}/` : '';
    const generationKey = this.generationKey();
    let token: string | undefined;
    do {
      const res = await this.client.send(
        new ListObjectsV2Command({ Bucket: this.bucket, Prefix: prefix, ContinuationToken: token }),
      );
      const contents = (res.Contents ?? []) as Array<{ Key?: string }>;
      const objects = contents
        .filter((o) => o.Key && o.Key !== generationKey)
        .map((o) => ({ Key: o.Key! }));
      for (let i = 0; i < objects.length; i += 1000) {
        await this.assertGeneration();
        await this.client.send(
          new DeleteObjectsCommand({
            Bucket: this.bucket,
            Delete: { Objects: objects.slice(i, i + 1000), Quiet: true },
          }),
        );
      }
      token = res.IsTruncated ? (res.NextContinuationToken ?? undefined) : undefined;
    } while (token);
  }
}

/**
 * Builds an S3ReplicaClient from parsed replica URL parts. `host` is the bucket,
 * `urlPath` is the prefix, `query` may carry `region`/`endpoint`, and `userinfo`
 * carries `accessKeyId:secretAccessKey`.
 */
export function newReplicaClientFromURL(
  _scheme: string,
  host: string,
  urlPath: string,
  query: URLSearchParams | null,
  userinfo: string | null,
): ReplicaClient {
  const opts: S3ReplicaClientOptions = {
    bucket: host,
    prefix: normalizePrefix(urlPath),
    region: query?.get('region') ?? undefined,
    endpoint: query?.get('endpoint') ?? undefined,
  };
  if (userinfo) {
    const [accessKeyId, secretAccessKey] = userinfo.split(':');
    opts.accessKeyId = accessKeyId;
    opts.secretAccessKey = secretAccessKey;
  }
  return new S3ReplicaClient(opts);
}

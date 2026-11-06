/**
 * In-memory S3 fake for tests. Implements the minimal `S3ClientLike` contract:
 * a `send(command)` method that handles the AWS SDK v3 commands used by
 * {@link S3ReplicaClient}. Objects live in a Map keyed by `bucket/key`.
 *
 * Conditional requests follow S3:
 *  - PutObject and CompleteMultipartUpload honor `If-None-Match: *` (fails if
 *    the key exists) and `If-Match: <etag>` (fails if the key is missing or has
 *    another ETag) with `412 PreconditionFailed`. A multipart upload whose
 *    completion fails a precondition stays open, so it can be retried.
 *  - GetObject and HeadObject honor `If-Match` (412) and `If-None-Match`
 *    (`304 NotModified`).
 *  - DeleteObject honors `If-Match`.
 * ListObjectsV2 and HeadObject report each object's ETag.
 *
 * `onSend` runs before every command; tests use it to pause or fail requests.
 */

import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  UploadPartCommand,
} from '@aws-sdk/client-s3';

interface StoredObject {
  body: Buffer;
  metadata: Record<string, string>;
  lastModified: Date;
  /** Defaults to the MD5 of the body (tests may store objects directly). */
  etag?: string;
}

interface MultipartUpload {
  bucket: string;
  key: string;
  parts: Map<number, Buffer>;
  metadata: Record<string, string>;
}

/** Normalizes an SDK Body value to a Buffer. */
function toBuffer(raw: unknown): Buffer {
  if (Buffer.isBuffer(raw)) return Buffer.from(raw);
  if (raw instanceof Uint8Array) return Buffer.from(raw);
  if (raw !== undefined && raw !== null) return Buffer.from(String(raw));
  return Buffer.alloc(0);
}

export class MemoryS3 {
  readonly objects = new Map<string, StoredObject>();
  readonly multiparts = new Map<string, MultipartUpload>();
  private uploadSeq = 0;
  /** Called before each command is handled (e.g. to delay or fail it). */
  onSend: ((command: any) => void | Promise<void>) | null = null;

  async send(command: any): Promise<any> {
    if (this.onSend !== null) await this.onSend(command);
    if (command instanceof ListObjectsV2Command) {
      return this.list(command);
    }
    if (command instanceof GetObjectCommand) {
      return this.get(command);
    }
    if (command instanceof PutObjectCommand) {
      return this.put(command);
    }
    if (command instanceof DeleteObjectCommand) {
      return this.deleteOne(command);
    }
    if (command instanceof DeleteObjectsCommand) {
      return this.delete(command);
    }
    if (command instanceof HeadObjectCommand) {
      return this.head(command);
    }
    if (command instanceof HeadBucketCommand) {
      return {};
    }
    if (command instanceof CreateMultipartUploadCommand) {
      return this.createMultipart(command);
    }
    if (command instanceof UploadPartCommand) {
      return this.uploadPart(command);
    }
    if (command instanceof CompleteMultipartUploadCommand) {
      return this.completeMultipart(command);
    }
    if (command instanceof AbortMultipartUploadCommand) {
      return this.abortMultipart(command);
    }
    throw new Error(`MemoryS3: unsupported command ${command?.constructor?.name}`);
  }

  private createMultipart(cmd: any) {
    const uploadId = `upload-${++this.uploadSeq}`;
    this.multiparts.set(uploadId, {
      bucket: cmd.input.Bucket,
      key: cmd.input.Key,
      parts: new Map(),
      metadata: cmd.input.Metadata ?? {},
    });
    return { UploadId: uploadId };
  }

  private uploadPart(cmd: any) {
    const mp = this.multiparts.get(cmd.input.UploadId);
    if (!mp) throw noSuchUpload(cmd.input.UploadId);
    const body = toBuffer(cmd.input.Body);
    mp.parts.set(cmd.input.PartNumber, body);
    return { ETag: `fake-etag-${cmd.input.PartNumber}` };
  }

  private completeMultipart(cmd: any) {
    const mp = this.multiparts.get(cmd.input.UploadId);
    if (!mp) throw noSuchUpload(cmd.input.UploadId);
    const k = this.key(mp.bucket, mp.key);
    this.checkWritePreconditions(k, cmd.input);
    const partNumbers = [...mp.parts.keys()].sort((a, b) => a - b);
    const chunks = partNumbers.map((n) => mp.parts.get(n)!);
    const body = Buffer.concat(chunks);
    const etag = `"${createHash('md5').update(body).digest('hex')}-${partNumbers.length}"`;
    this.objects.set(k, {
      body,
      metadata: mp.metadata,
      lastModified: new Date(),
      etag,
    });
    this.multiparts.delete(cmd.input.UploadId);
    return { ETag: etag, Key: mp.key };
  }

  private abortMultipart(cmd: any) {
    this.multiparts.delete(cmd.input.UploadId);
    return {};
  }

  private key(bucket: string, key: string): string {
    return `${bucket}/${key}`;
  }

  /** `If-None-Match: *` / `If-Match` of a write (PutObject, CompleteMultipartUpload). */
  private checkWritePreconditions(k: string, input: { IfNoneMatch?: string; IfMatch?: string }): void {
    const existing = this.objects.get(k);
    if (input.IfNoneMatch !== undefined && input.IfNoneMatch !== null) {
      if (input.IfNoneMatch !== '*') throw new Error('MemoryS3: If-None-Match only supports "*" on writes');
      if (existing) throw precondition(k);
    }
    if (input.IfMatch !== undefined && input.IfMatch !== null) {
      if (!existing || etagOf(existing) !== input.IfMatch) throw precondition(k);
    }
  }

  /** `If-Match` (412) / `If-None-Match` (304) of a read (GetObject, HeadObject). */
  private checkReadPreconditions(k: string, obj: StoredObject, input: { IfNoneMatch?: string; IfMatch?: string }): void {
    if (input.IfMatch !== undefined && input.IfMatch !== null && input.IfMatch !== etagOf(obj)) {
      throw precondition(k);
    }
    if (input.IfNoneMatch !== undefined && input.IfNoneMatch !== null) {
      if (input.IfNoneMatch === '*' || input.IfNoneMatch === etagOf(obj)) throw notModified(k);
    }
  }

  private list(cmd: any) {
    const prefix = cmd.input.Prefix ?? '';
    const contents: any[] = [];
    for (const [k, obj] of this.objects) {
      const slash = k.indexOf('/');
      const bucket = k.slice(0, slash);
      if (bucket !== cmd.input.Bucket) continue;
      const key = k.slice(slash + 1);
      if (!key.startsWith(prefix)) continue;
      contents.push({
        Key: key,
        Size: obj.body.length,
        LastModified: obj.lastModified,
        ETag: etagOf(obj),
      });
    }
    return { Contents: contents, IsTruncated: false };
  }

  private get(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const obj = this.objects.get(k);
    if (!obj) throw notFound(cmd.input.Key);
    this.checkReadPreconditions(k, obj, cmd.input);
    let body = obj.body;
    if (cmd.input.Range) {
      const m = /^bytes=(\d+)-(\d*)$/.exec(cmd.input.Range);
      if (m) {
        const start = Number(m[1]);
        // Like S3: a range starting past the end fails, one ending past it is cut
        if (start >= obj.body.length) throw invalidRange(cmd.input.Range);
        const end = m[2] === '' ? obj.body.length - 1 : Math.min(Number(m[2]), obj.body.length - 1);
        body = obj.body.subarray(start, end + 1);
      }
    }
    return {
      Body: Readable.from(body),
      ContentLength: body.length,
      LastModified: obj.lastModified,
      Metadata: obj.metadata,
      ETag: etagOf(obj),
    };
  }

  private put(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const raw = cmd.input.Body;
    const body = Buffer.isBuffer(raw)
      ? Buffer.from(raw)
      : raw instanceof Uint8Array
        ? Buffer.from(raw)
        : raw !== undefined
          ? Buffer.from(String(raw))
          : Buffer.alloc(0);
    const etag = fakeEtag(body);
    this.checkWritePreconditions(k, cmd.input);

    this.objects.set(k, {
      body,
      metadata: cmd.input.Metadata ?? {},
      lastModified: new Date(),
      etag,
    });
    return { ETag: etag, ContentLength: body.length };
  }

  private deleteOne(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    if (cmd.input.IfMatch !== undefined && cmd.input.IfMatch !== null) {
      const existing = this.objects.get(k);
      if (!existing) throw notFound(k);
      if (etagOf(existing) !== cmd.input.IfMatch) throw precondition(k);
    }
    const removed = this.objects.delete(k);
    if (!removed) throw notFound(k);
    return {};
  }

  private delete(cmd: any) {
    for (const obj of cmd.input.Delete.Objects ?? []) {
      this.objects.delete(this.key(cmd.input.Bucket, obj.Key));
    }
    return { Deleted: cmd.input.Delete.Objects ?? [] };
  }

  private head(cmd: any) {
    const k = this.key(cmd.input.Bucket, cmd.input.Key);
    const obj = this.objects.get(k);
    if (!obj) throw notFound(cmd.input.Key);
    this.checkReadPreconditions(k, obj, cmd.input);
    return {
      ContentLength: obj.body.length,
      Metadata: obj.metadata,
      LastModified: obj.lastModified,
      ETag: etagOf(obj),
    };
  }
}

function notFound(key: string): Error {
  const err = new Error(`Not Found: ${key}`);
  (err as any).name = 'NotFound';
  (err as any).$metadata = { httpStatusCode: 404 };
  return err;
}

function invalidRange(range: string): Error {
  const err = new Error(`The requested range is not satisfiable: ${range}`);
  (err as any).name = 'InvalidRange';
  (err as any).$metadata = { httpStatusCode: 416 };
  return err;
}

function noSuchUpload(uploadId: string): Error {
  const err = new Error(`No Such Upload: ${uploadId}`);
  (err as any).name = 'NoSuchUpload';
  (err as any).$metadata = { httpStatusCode: 404 };
  return err;
}

function notModified(key: string): Error {
  const err = new Error(`Not Modified: ${key}`);
  (err as any).name = 'NotModified';
  (err as any).$metadata = { httpStatusCode: 304 };
  return err;
}

function precondition(key: string): Error {
  const err = new Error(`Precondition Failed: ${key}`);
  (err as any).name = 'PreconditionFailed';
  (err as any).$metadata = { httpStatusCode: 412 };
  return err;
}

function etagOf(obj: StoredObject): string {
  return obj.etag ?? fakeEtag(obj.body);
}

function fakeEtag(body: Buffer): string {
  return `"${createHash('md5').update(body).digest('hex')}"`;
}

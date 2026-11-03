/**
 * S3 Leaser — distributed lease backed by an S3 `lock.json` object.
 *
 * Mirrors litestream's `s3/leaser.go`. Acquire uses `If-None-Match: *` when no
 * lease exists (and `If-Match: <etag>` when replacing an expired lease), so two
 * racing acquirers cannot both win; one gets `412 PreconditionFailed`.
 */

import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

import {
  DefaultLeasePath,
  DefaultLeaseTTL,
  ErrLeaseNotHeld,
  Lease,
  LeaseExistsError,
  type Leaser,
} from '@platformatic/sqlite-js-stream';

import type { S3ClientLike } from './replica-client.ts';
import { isNotFound, isPreconditionFailed, readBody } from './util.ts';

/** S3Leaser acquires/renews/releases a lease object in an S3 bucket. */
export class S3Leaser implements Leaser {
  readonly bucket: string;
  readonly prefix: string;
  readonly key: string;
  private client: S3ClientLike;
  private owner: string;
  private ttlMs: number;

  constructor(opts: { bucket: string; prefix?: string; client: S3ClientLike; owner?: string; ttlMs?: number }) {
    if (!opts.bucket) throw new Error('S3 leaser: bucket is required');
    this.bucket = opts.bucket;
    this.prefix = opts.prefix?.replace(/^\/+/, '').replace(/\/+$/, '') ?? '';
    this.key = this.prefix ? `${this.prefix}/${DefaultLeasePath}` : DefaultLeasePath;
    this.client = opts.client;
    this.owner = opts.owner ?? `${process.pid}`;
    this.ttlMs = opts.ttlMs ?? DefaultLeaseTTL;
  }

  async acquireLease(): Promise<Lease> {
    const existing = await this.readLease();
    if (existing !== null && !existing.isExpired()) {
      throw new LeaseExistsError(existing.owner, existing.expiresAt);
    }

    let generation = 1;
    if (existing !== null) generation = existing.generation + 1;

    const lease = new Lease(generation, Date.now() + this.ttlMs, this.owner);
    const data = new TextEncoder().encode(
      JSON.stringify({
        generation: lease.generation,
        expires_at: new Date(lease.expiresAt).toISOString(),
        owner: lease.owner,
      }),
    );

    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.key,
          Body: data,
          ContentType: 'application/json',
          ...(existing === null ? { IfNoneMatch: '*' } : { IfMatch: existing.etag ?? '' }),
        }),
      );
    } catch (err) {
      if (isPreconditionFailed(err)) {
        const current = await this.readLease();
        if (current !== null) throw new LeaseExistsError(current.owner, current.expiresAt);
      }
      throw err;
    }

    return (await this.readLease()) ?? lease;
  }

  async renewLease(lease: Lease): Promise<Lease> {
    if (lease.etag == null) throw new ErrLeaseNotHeld();
    const renewed = new Lease(lease.generation, Date.now() + this.ttlMs, lease.owner);
    const data = new TextEncoder().encode(
      JSON.stringify({
        generation: renewed.generation,
        expires_at: new Date(renewed.expiresAt).toISOString(),
        owner: renewed.owner,
      }),
    );
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: this.key,
          Body: data,
          ContentType: 'application/json',
          IfMatch: lease.etag,
        }),
      );
    } catch (err) {
      if (isPreconditionFailed(err)) throw new ErrLeaseNotHeld();
      throw err;
    }
    return (await this.readLease()) ?? renewed;
  }

  async releaseLease(lease: Lease): Promise<void> {
    if (lease.etag == null) throw new ErrLeaseNotHeld();
    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucket,
          Key: this.key,
          IfMatch: lease.etag,
        }),
      );
    } catch (err) {
      if (isNotFound(err)) return; // already released
      if (isPreconditionFailed(err)) throw new ErrLeaseNotHeld();
      throw err;
    }
  }

  private async readLease(): Promise<Lease | null> {
    try {
      const res = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: this.key }),
      );
      const data = await readBody(res.Body);
      const parsed = JSON.parse(new TextDecoder().decode(data)) as {
        generation?: number;
        expires_at?: string;
        owner?: string;
      };
      const expiresAt = Date.parse(parsed.expires_at ?? '');
      if (Number.isNaN(expiresAt)) throw new Error('invalid lease file: bad expires_at');
      const etag = typeof res.ETag === 'string' ? res.ETag : null;
      return new Lease(parsed.generation ?? 1, expiresAt, parsed.owner ?? '', etag);
    } catch (err) {
      if (isNotFound(err)) return null;
      throw err;
    }
  }
}

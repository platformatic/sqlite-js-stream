/**
 * PgLeaser — distributed lease backed by a PostgreSQL row.
 *
 * Mirrors litestream's `s3/leaser.go`. The lock.json fields (`generation`,
 * `owner`, `expires_at`) are stored in a `ltx_leases` row, and a `version`
 * column is used for compare-and-swap in place of the S3 `ETag`. Two racing
 * acquirers cannot both win: one updates/inserts the row and increments the
 * version, the other sees a version (or row) that no longer matches and throws
 * {@link LeaseExistsError}.
 */

import type { Sql } from 'postgres';

import {
  DefaultLeasePath,
  DefaultLeaseTTL,
  ErrLeaseNotHeld,
  InvalidArgumentError,
  Lease,
  LeaseExistsError,
  type Leaser,
} from '@platformatic/sqlite-js-stream';

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface PgLeaserOptions {
  /** Injected `postgres()` client. */
  sql: Sql;
  /** Lease row prefix. Defaults to `''`. */
  prefix?: string;
  /** Schema holding the `ltx_leases` table. Defaults to `public`. */
  schema?: string;
  /** Base table-name prefix. Defaults to `ltx`. */
  tablePrefix?: string;
  /** Owner identifier. Defaults to the process id. */
  owner?: string;
  /** Lease TTL in ms. Defaults to {@link DefaultLeaseTTL}. */
  ttlMs?: number;
}

/** Normalizes a prefix (no leading/trailing slash). */
function normalizePrefix(prefix: string): string {
  return prefix.replace(/^\/+/, '').replace(/\/+$/, '');
}

/** PgLeaser acquires/renews/releases a lease row in PostgreSQL. */
export class PgLeaser implements Leaser {
  readonly prefix: string;
  readonly key: string;
  private readonly sql: Sql;
  private readonly schema: string;
  private readonly tablePrefix: string;
  private readonly owner: string;
  private readonly ttlMs: number;
  private tableCreated = false;

  constructor(opts: PgLeaserOptions) {
    if (!opts.sql) throw new InvalidArgumentError('pg leaser: sql client is required');
    this.sql = opts.sql;
    this.prefix = normalizePrefix(opts.prefix ?? '');
    this.schema = opts.schema ?? 'public';
    this.tablePrefix = opts.tablePrefix ?? 'ltx';
    this.owner = opts.owner ?? `${process.pid}`;
    this.ttlMs = opts.ttlMs ?? DefaultLeaseTTL;
    this.key = this.prefix ? `${this.prefix}/${DefaultLeasePath}` : DefaultLeasePath;
    for (const ident of [this.schema, this.tablePrefix]) {
      if (!IDENT_RE.test(ident)) {
        throw new InvalidArgumentError(
          `pg leaser: invalid identifier "${ident}" (schema/tablePrefix must be plain SQL identifiers)`,
        );
      }
    }
  }

  /** Returns the schema-qualified `ltx_leases` table reference. */
  private table(): string {
    return `${this.schema}.${this.tablePrefix}_leases`;
  }

  /** Creates the lease table once (idempotent). */
  private async ensureTable(): Promise<void> {
    if (this.tableCreated) return;
    await this.sql`
      CREATE TABLE IF NOT EXISTS ${this.sql(this.table())} (
        prefix TEXT PRIMARY KEY,
        generation BIGINT NOT NULL,
        owner TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        version BIGINT NOT NULL
      )
    `;
    this.tableCreated = true;
  }

  async acquireLease(): Promise<Lease> {
    await this.ensureTable();
    const existing = await this.readLease();

    if (existing !== null && !existing.isExpired()) {
      throw new LeaseExistsError(existing.owner, existing.expiresAt);
    }

    const generation = existing !== null ? existing.generation + 1 : 1;
    const lease = new Lease(generation, Date.now() + this.ttlMs, this.owner);
    const expires = new Date(lease.expiresAt);

    if (existing !== null) {
      // Replace a stale/expired lease with a compare-and-swap on the version.
      const res = await this.sql`
        UPDATE ${this.sql(this.table())}
        SET generation = ${generation}, owner = ${this.owner},
            expires_at = ${expires}, version = version + 1
        WHERE prefix = ${this.prefix} AND version = ${Number(existing.etag!)}
        RETURNING version
      `;
      if (res.length === 0) {
        const current = await this.readLease();
        if (current !== null) {
          throw new LeaseExistsError(current.owner, current.expiresAt);
        }
        throw new LeaseExistsError(this.owner, Date.now());
      }
    } else {
      // No existing lease: insert, no-op on a concurrent conflict.
      const res = await this.sql`
        INSERT INTO ${this.sql(this.table())} (prefix, generation, owner, expires_at, version)
        VALUES (${this.prefix}, ${generation}, ${this.owner}, ${expires}, 1)
        ON CONFLICT (prefix) DO NOTHING
        RETURNING version
      `;
      if (res.length === 0) {
        const current = await this.readLease();
        if (current !== null) {
          throw new LeaseExistsError(current.owner, current.expiresAt);
        }
        throw new LeaseExistsError(this.owner, Date.now());
      }
    }

    return (await this.readLease()) ?? lease;
  }

  async renewLease(lease: Lease): Promise<Lease> {
    await this.ensureTable();
    if (lease.etag == null) throw new ErrLeaseNotHeld();

    const renewed = new Lease(lease.generation, Date.now() + this.ttlMs, lease.owner);
    const res = await this.sql`
      UPDATE ${this.sql(this.table())}
      SET expires_at = ${new Date(renewed.expiresAt)}, version = version + 1
      WHERE prefix = ${this.prefix} AND owner = ${this.owner}
        AND version = ${Number(lease.etag)}
      RETURNING version
    `;
    if (res.length === 0) throw new ErrLeaseNotHeld();
    return (await this.readLease()) ?? renewed;
  }

  async releaseLease(lease: Lease): Promise<void> {
    await this.ensureTable();
    if (lease.etag == null) throw new ErrLeaseNotHeld();

    const res = await this.sql`
      DELETE FROM ${this.sql(this.table())}
      WHERE prefix = ${this.prefix} AND owner = ${this.owner}
        AND version = ${Number(lease.etag)}
      RETURNING version
    `;
    if (res.length === 0) {
      // Either already released (row gone) or another host took over.
      const current = await this.readLease();
      if (current !== null) throw new ErrLeaseNotHeld();
      // Already released → idempotent no-op.
    }
  }

  private async readLease(): Promise<Lease | null> {
    const rows = await this.sql`
      SELECT generation, owner, expires_at, version
      FROM ${this.sql(this.table())}
      WHERE prefix = ${this.prefix}
    `;
    if (rows.length === 0) return null;
    const row = rows[0];
    return new Lease(
      Number(row.generation),
      new Date(row.expires_at).getTime(),
      row.owner ?? '',
      String(row.version),
    );
  }
}


/**
 * Serializes the creation of the LTX tables. `CREATE TABLE IF NOT EXISTS` is
 * not safe under concurrency in PostgreSQL: two sessions creating the same
 * table at once can fail with a unique violation on `pg_type` (23505) or
 * "relation already exists" (42P07), which happens when several processes
 * start against a fresh database. The DDL runs in a transaction holding an
 * advisory lock keyed by the schema and table prefix, so the second session
 * waits and then finds the tables.
 */

import type { ISql, Sql } from 'postgres';

export async function withDdlLock(
  sql: Sql,
  schema: string,
  tablePrefix: string,
  fn: (tx: ISql) => Promise<void>,
): Promise<void> {
  await sql.begin(async (tx) => {
    await tx`SELECT pg_advisory_xact_lock(hashtext(${`sqlite-js-stream:${schema}.${tablePrefix}`}))`;
    await fn(tx);
  });
}

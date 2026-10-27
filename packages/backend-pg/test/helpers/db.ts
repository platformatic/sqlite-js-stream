/**
 * Test helper: connect to a PostgreSQL instance for the backend tests.
 *
 * Connection URL is read from `PG_URL` (set by CI's Postgres service), falling
 * back to a local default. Notices are suppressed so `CREATE TABLE IF NOT EXISTS`
 * does not spam the test output.
 */

import type { TestContext } from 'node:test';

import postgres, { type Sql } from 'postgres';

/** Returns the Postgres URL used by the tests. */
export function pgUrl(): string {
  return (
    process.env.PG_URL ?? 'postgres://postgres:postgres@localhost:5432/postgres'
  );
}

/** Connects to the test Postgres instance. */
export function connect(): Sql {
  return postgres(pgUrl(), {
    onnotice: () => {},
    max: 10,
  });
}

/**
 * Creates a dedicated `postgres()` client tied to a test, and registers an
 * `after` callback to close it so the process can exit cleanly. Using a fresh
 * client per test (rather than a module-level shared pool) keeps the test
 * runner's event loop from being held open by idle database sockets.
 */
export function connectForTest(t: TestContext): Sql {
  const sql = connect();
  t.after(async () => {
    await sql.end({ timeout: 1 });
  });
  return sql;
}

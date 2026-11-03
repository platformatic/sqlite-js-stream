import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Sql } from 'postgres';

import { PgLeaser, PgReplicaClient } from '../src/index.ts';
import { connectForTest } from './helpers/db.ts';

test('concurrent init() on a fresh database creates the tables once', { skip: process.env.PG_URL ? false : 'PG_URL is not set' }, async (t) => {
  // A table prefix no other test uses, so the tables do not exist yet.
  const tablePrefix = `init_${Date.now().toString(36)}_${process.pid}`;

  // `after` hooks run in the order they are registered: drop the tables before
  // connectForTest's hooks close the connections.
  const conns: Sql[] = [];
  t.after(async () => {
    const sql = conns[0];
    for (const name of ['files', 'chunks', 'generations', 'leases']) {
      await sql`DROP TABLE IF EXISTS ${sql(`public.${tablePrefix}_${name}`)}`;
    }
  });
  for (let i = 0; i < 8; i++) conns.push(connectForTest(t));

  const results = await Promise.allSettled(
    conns.flatMap((sql, i) => [
      new PgReplicaClient({ sql, prefix: `p${i}`, tablePrefix }).init(),
      new PgLeaser({ sql, prefix: `p${i}`, tablePrefix, owner: `o${i}` }).acquireLease().then(() => {}),
    ]),
  );
  for (const r of results) {
    assert.equal(r.status, 'fulfilled', r.status === 'rejected' ? String(r.reason) : '');
  }
});

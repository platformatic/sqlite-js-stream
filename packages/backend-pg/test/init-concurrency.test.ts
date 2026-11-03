import { test } from 'node:test';
import assert from 'node:assert/strict';

import { PgLeaser, PgReplicaClient } from '../src/index.ts';
import { connectForTest } from './helpers/db.ts';

test('concurrent init() on a fresh database creates the tables once', { skip: process.env.PG_URL ? false : 'PG_URL is not set' }, async (t) => {
  // A table prefix no other test uses, so the tables do not exist yet.
  const tablePrefix = `init_${Date.now().toString(36)}_${process.pid}`;
  const conns = Array.from({ length: 8 }, () => connectForTest(t));

  try {
    const results = await Promise.allSettled(
      conns.flatMap((sql, i) => [
        new PgReplicaClient({ sql, prefix: `p${i}`, tablePrefix }).init(),
        new PgLeaser({ sql, prefix: `p${i}`, tablePrefix, owner: `o${i}` }).acquireLease().then(() => {}),
      ]),
    );
    for (const r of results) {
      assert.equal(r.status, 'fulfilled', r.status === 'rejected' ? String(r.reason) : '');
    }
  } finally {
    const sql = conns[0];
    for (const name of ['files', 'chunks', 'generations', 'leases']) {
      await sql`DROP TABLE IF EXISTS ${sql(`public.${tablePrefix}_${name}`)}`;
    }
  }
});

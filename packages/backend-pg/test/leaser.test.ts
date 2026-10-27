import { test, describe, type TestContext } from 'node:test';
import assert from 'node:assert/strict';

import { ErrLeaseNotHeld, LeaseExistsError } from '@platformatic/sqlite-js-stream';

import { PgLeaser } from '../src/leaser.ts';
import { connectForTest } from './helpers/db.ts';

function makeLeaser(
  t: TestContext,
  opts: { prefix?: string; owner?: string; ttlMs?: number } = {},
): PgLeaser {
  return new PgLeaser({ sql: connectForTest(t), prefix: opts.prefix, owner: opts.owner, ttlMs: opts.ttlMs });
}

describe('PgLeaser', () => {
  test('acquire → held → renew → release → reacquire', async (t) => {
    const leaser = makeLeaser(t, { prefix: `lease/${Date.now().toString(36)}` });

    const lease = await leaser.acquireLease();
    assert.equal(lease.generation, 1);
    assert.ok(!lease.isExpired());
    assert.ok(lease.etag);

    await assert.rejects(leaser.acquireLease(), LeaseExistsError);

    const renewed = await leaser.renewLease(lease);
    assert.equal(renewed.generation, 1);
    assert.notEqual(renewed.etag, lease.etag, 'version should advance on renewal');
    assert.ok(!renewed.isExpired());

    await leaser.releaseLease(renewed);

    const lease2 = await leaser.acquireLease();
    assert.equal(lease2.generation, 1);
    await leaser.releaseLease(lease2);
  });

  test('a second leaser cannot acquire while held; can after release', async (t) => {
    const prefix = `lease/${Date.now().toString(36)}`;
    const sql = connectForTest(t);
    const a = new PgLeaser({ sql, prefix, owner: 'a' });
    const b = new PgLeaser({ sql, prefix, owner: 'b' });

    const leaseA = await a.acquireLease();
    await assert.rejects(b.acquireLease(), LeaseExistsError);

    await a.releaseLease(leaseA);
    const leaseB = await b.acquireLease();
    assert.equal(leaseB.owner, 'b');
    await b.releaseLease(leaseB);
  });

  test('renew after another host takes over throws ErrLeaseNotHeld', async (t) => {
    const prefix = `lease/${Date.now().toString(36)}`;
    const sql = connectForTest(t);
    const a = new PgLeaser({ sql, prefix, owner: 'a' });
    const b = new PgLeaser({ sql, prefix, owner: 'b' });

    const leaseA = await a.acquireLease();

    // Force-expire the lease row, then let `b` take it over.
    await sql`
      UPDATE ${sql('public.ltx_leases')}
      SET expires_at = now() - interval '1 hour'
      WHERE prefix = ${prefix}
    `;
    const leaseB = await b.acquireLease();

    await assert.rejects(a.renewLease(leaseA), ErrLeaseNotHeld);
    assert.equal(leaseB.generation, 2);
    await b.releaseLease(leaseB);
  });

  test('release is a no-op when the lease is already gone', async (t) => {
    const leaser = makeLeaser(t, { prefix: `lease/${Date.now().toString(36)}` });
    const lease = await leaser.acquireLease();
    await leaser.releaseLease(lease);
    // Second release is already released → no error.
    await leaser.releaseLease(lease);
  });

  test('a stale release after another host takes over throws ErrLeaseNotHeld', async (t) => {
    const prefix = `lease/${Date.now().toString(36)}`;
    const sql = connectForTest(t);
    const a = new PgLeaser({ sql, prefix, owner: 'a' });
    const b = new PgLeaser({ sql, prefix, owner: 'b' });

    const leaseA = await a.acquireLease();
    await a.releaseLease(leaseA);

    // `b` acquires a fresh lease. Because the tokens differ between holders,
    // `a`'s stale release must not delete `b`'s lease.
    const leaseB = await b.acquireLease();
    await assert.rejects(a.releaseLease(leaseA), ErrLeaseNotHeld);

    // `b`'s lease is still held.
    await assert.rejects(a.acquireLease(), LeaseExistsError);
    await b.releaseLease(leaseB);
  });
});

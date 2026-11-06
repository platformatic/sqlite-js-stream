import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { mkdir, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';

import { FileLeaser, FileReplicaClient } from '../../../src/index.ts';
import { deletePrefixSuite } from '../../helpers/delete-prefix-suite.ts';

deletePrefixSuite('file', (t) => {
  const base = mkdtempSync(join(tmpdir(), 'sjs-delete-prefix-'));
  t.after(() => rmSync(base, { recursive: true, force: true }));
  return {
    root: () => 'root',
    client: (prefix) => new FileReplicaClient(join(base, prefix)),
    deletePrefix: async (prefix) => {
      const deleted = await FileReplicaClient.deletePrefix({ path: prefix === '' ? '' : join(base, prefix) });
      return deleted.map((p) => relative(base, p));
    },
    fenced: false,
    atomic: true,
  };
});

test('file deletePrefix removes the whole directory, leases and other files included', async (t) => {
  const base = mkdtempSync(join(tmpdir(), 'sjs-delete-prefix-'));
  t.after(() => rmSync(base, { recursive: true, force: true }));
  const ns = join(base, 'ns');

  // A replica holding only a lease, and a directory that is no replica.
  await mkdir(join(ns, 'leased'), { recursive: true });
  await new FileLeaser(join(ns, 'leased')).acquireLease();
  await mkdir(join(ns, 'plain'), { recursive: true });
  await writeFile(join(ns, 'plain', 'notes.txt'), 'not a replica');

  assert.deepEqual(await FileReplicaClient.deletePrefix({ path: ns }), [join(ns, 'leased')]);
  await assert.rejects(stat(ns), { code: 'ENOENT' });
});

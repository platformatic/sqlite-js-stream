import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { tempDir } from './helpers/source.ts';

const EXAMPLE = new URL('../example/', import.meta.url);
const FILES = ['create-sample.ts', 'main.ts', 'worker.ts'];

test('the example restores a database while workers query it', (t) => {
  const cwd = tempDir(t);
  let output = '';

  for (const file of ['create-sample.ts', 'main.ts']) {
    const child = spawnSync(process.execPath, [fileURLToPath(new URL(file, EXAMPLE))], {
      cwd,
      encoding: 'utf8',
      timeout: 60_000,
    });

    assert.equal(child.status, 0, child.stderr);
    output += child.stdout;
  }

  assert.match(output, /email: 'user499999@example\.com'/);
  assert.match(output, /Answered in \d+ ms, with \d+ pages requested on demand/);
  assert.match(output, /Complete: true/);
  assert.match(output, /Integrity check: ok/);
});

test('the README shows the example as it is', () => {
  const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');

  for (const file of FILES) {
    const code = readFileSync(new URL(file, EXAMPLE), 'utf8')
      .replace("from '../src/index.ts'", "from '@platformatic/sqlite-js-stream-paged-vfs'");

    assert.ok(readme.includes('```ts\n' + code + '```\n'), `README.md does not include example/${file} as it is`);
  }
});

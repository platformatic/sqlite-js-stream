import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { Crc64, crc64ISO } from '../../src/ltx/crc64.ts';
import { ReferenceCrc64 } from '../helpers/crc64-reference.ts';

test('crc64 ISO matches the published check value for "123456789"', () => {
  assert.equal(crc64ISO(new TextEncoder().encode('123456789')), 0xb90956c775a41001n);
});

test('crc64 is incremental (chunked writes equal a single write)', () => {
  const data = new TextEncoder().encode('hello world, this is a crc64 test buffer!');
  const whole = crc64ISO(data);

  const h = new Crc64();
  h.update(data.subarray(0, 7));
  h.update(data.subarray(7, 20));
  h.update(data.subarray(20));
  assert.equal(h.sum64(), whole);
});

test('crc64 reset returns to initial state', () => {
  const h = new Crc64();
  h.update(new TextEncoder().encode('aaa'));
  const first = h.sum64();
  h.reset();
  h.update(new TextEncoder().encode('aaa'));
  assert.equal(h.sum64(), first);
});

test('crc64 of empty buffer is the init/xorout complement', () => {
  assert.equal(crc64ISO(new Uint8Array(0)), 0n);
});

test('crc64 equals the BigInt reference on random buffers, whole and in chunks', () => {
  for (const size of [1, 7, 8, 9, 255, 256, 4096, 65536]) {
    const data = new Uint8Array(randomBytes(size));
    const reference = new ReferenceCrc64();
    reference.update(data);
    assert.equal(crc64ISO(data), reference.sum64(), `size ${size}`);

    const h = new Crc64();
    const split = Math.floor(size / 3);
    h.update(data.subarray(0, split));
    h.update(data.subarray(split));
    assert.equal(h.sum64(), reference.sum64(), `size ${size} in chunks`);
  }

  for (const byte of [0x00, 0xff, 0x80]) {
    const data = new Uint8Array(4096).fill(byte);
    const reference = new ReferenceCrc64();
    reference.update(data);
    assert.equal(crc64ISO(data), reference.sum64(), `byte ${byte}`);
  }
});

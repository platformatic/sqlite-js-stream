import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { compressBlock, decompressBlock } from '../../src/ltx/index.ts';
import { byteEq } from '../helpers/byte.ts';
import { referenceCompressBlock } from '../helpers/lz4-reference.ts';

test('lz4 block round-trips compressible data', () => {
  const data = new Uint8Array(512);
  for (let i = 0; i < 512; i++) data[i] = (5 + (i % 7)) & 0xff;

  const compressed = compressBlock(data);
  assert.ok(compressed.length < data.length, `expected some compression, got ${compressed.length}`);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});

test('lz4 block round-trips random data (likely literal-only path)', () => {
  const data = new Uint8Array(randomBytes(512));

  const compressed = compressBlock(data);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});

test('lz4 block round-trips empty data', () => {
  const data = new Uint8Array(0);
  const compressed = compressBlock(data);
  assert.equal(compressed.length, 1);
  const out = decompressBlock(compressed, 0);
  assert.equal(out.length, 0);
});

test('lz4 block round-trips a 4KiB page', () => {
  const data = new Uint8Array(4096);
  for (let i = 0; i < 4096; i++) data[i] = (i & 0xff);
  const compressed = compressBlock(data);
  const out = decompressBlock(compressed, data.length);
  assert.ok(byteEq(out, data));
});

function toHex(b: Uint8Array): string {
  return Buffer.from(b).toString('hex');
}

test('lz4 compressBlock is byte-identical to Go pierrec/lz4/v4', () => {
  // Vectors produced by Go's `lz4.CompressBlock` (internal/lz4block Fast mode),
  // captured by running `tools/golden` / a minimal Go program.
  const vectors: Array<[string, string]> = [
    ['hello world hello world hello world hello world', 'cf68656c6c6f20776f726c64200c0001001800b068656c6c6f20776f726c64'],
    ['a'.repeat(47), '1f61010009000200e06161616161616161616161616161'],
    [
      'The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.',
      'ff1e54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e202d0009002d00c06865206c617a7920646f672e',
    ],
  ];
  for (const [input, expectedHex] of vectors) {
    const compressed = compressBlock(new Uint8Array(Buffer.from(input, 'utf8')));
    assert.equal(toHex(compressed), expectedHex, `Go byte-identity for: ${JSON.stringify(input.slice(0, 24))}`);
  }
});

test('lz4 compressBlock equals the BigInt reference, byte for byte', () => {
  const inputs: Uint8Array[] = [];

  // Sizes around the limits of the scan, of random and of repeated bytes
  for (const size of [0, 1, 13, 14, 15, 16, 22, 23, 64, 255, 256, 4095, 4096, 4097, 65535, 65536, 65537, 200000]) {
    inputs.push(new Uint8Array(randomBytes(size)));
    inputs.push(new Uint8Array(size).fill(0xab));
    inputs.push(new Uint8Array(size));
  }

  // Pages like those of a database: text, small integers, runs, and matches at every distance
  let seed = 0x9e3779b9;
  const next = (): number => {
    seed ^= seed << 13;
    seed ^= seed >>> 17;
    seed ^= seed << 5;
    return seed >>> 0;
  };

  for (let page = 0; page < 300; page++) {
    const size = [512, 4096, 8192, 65536][page % 4];
    const data = new Uint8Array(size);
    let i = 0;

    while (i < size) {
      const kind = next() % 5;
      const length = Math.min(1 + (next() % 300), size - i);

      if (kind === 0) {
        data.fill(next() & 0xff, i, i + length);
      } else if (kind === 1 && i > 0) {
        const distance = 1 + (next() % Math.min(i, 70000));
        for (let j = 0; j < length; j++) data[i + j] = data[i + j - distance];
      } else if (kind === 2) {
        for (let j = 0; j < length; j++) data[i + j] = 97 + (next() % 26);
      } else if (kind === 3) {
        for (let j = 0; j < length; j++) data[i + j] = next() % 4;
      } else {
        for (let j = 0; j < length; j++) data[i + j] = next() & 0xff;
      }

      i += length;
    }

    inputs.push(data);
  }

  for (const [index, data] of inputs.entries()) {
    const compressed = compressBlock(data);
    assert.ok(byteEq(compressed, referenceCompressBlock(data)), `input ${index} of ${data.length} bytes`);
    assert.ok(byteEq(decompressBlock(compressed, data.length), data), `round trip of input ${index}`);
  }
});

/**
 * LTX page encoding benchmark.
 *
 * Every page written to an LTX file is checksummed (CRC-64 ISO) and compressed
 * (LZ4 block): this measures the throughput of both, the CPU cost of a sync
 * once the pages are read. Each is run on pages of text, which compress, and on
 * random pages, which do not.
 *
 * With BASELINE=1 the BigInt implementations they replaced, kept as test
 * helpers, are measured too, and the speedup is reported.
 *
 * Tunables (env):
 *   PAGES      pages in each set                         (default 2048)
 *   PAGE_SIZE  bytes of a page                           (default 4096)
 *   ROUNDS     timed rounds, the best one is reported    (default 5)
 *   BASELINE   also measure the BigInt implementations   (off by default)
 *
 * Run:
 *   pnpm bench:encode
 *   BASELINE=1 pnpm bench:encode
 *   PAGE_SIZE=65536 PAGES=256 pnpm bench:encode
 */

import { randomBytes } from 'node:crypto';

import { Crc64 } from '../src/ltx/crc64.ts';
import { compressBlock } from '../src/ltx/lz4.ts';
import { ReferenceCrc64 } from '../test/helpers/crc64-reference.ts';
import { referenceCompressBlock } from '../test/helpers/lz4-reference.ts';

const PAGES = Number(process.env.PAGES ?? 2048);
const PAGE_SIZE = Number(process.env.PAGE_SIZE ?? 4096);
const ROUNDS = Number(process.env.ROUNDS ?? 5);
const BASELINE = Boolean(process.env.BASELINE);

/** Pages of lowercase text with repetitions at several distances. */
function textPages(): Uint8Array[] {
  const pages: Uint8Array[] = [];
  for (let p = 0; p < PAGES; p++) {
    const page = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) page[i] = 97 + ((i * 7 + p * 13 + (i >> 5)) % 26);
    pages.push(page);
  }
  return pages;
}

function randomPages(): Uint8Array[] {
  const pages: Uint8Array[] = [];
  for (let p = 0; p < PAGES; p++) pages.push(new Uint8Array(randomBytes(PAGE_SIZE)));
  return pages;
}

/** Returns the best throughput of `fn` over the rounds, in MB/s, after a warm-up. */
function throughput(fn: () => void): number {
  const megabytes = (PAGES * PAGE_SIZE) / (1024 * 1024);
  let best = 0;
  fn();
  for (let round = 0; round < ROUNDS; round++) {
    const start = process.hrtime.bigint();
    fn();
    const seconds = Number(process.hrtime.bigint() - start) / 1e9;
    best = Math.max(best, megabytes / seconds);
  }
  return best;
}

interface Row {
  operation: string;
  pages: string;
  'MB/s': string;
  'BigInt MB/s'?: string;
  speedup?: string;
}

function row(operation: string, pages: string, current: number, baseline: number | null): Row {
  const result: Row = { operation, pages, 'MB/s': current.toFixed(1) };
  if (baseline !== null) {
    result['BigInt MB/s'] = baseline.toFixed(1);
    result.speedup = `${(current / baseline).toFixed(1)}x`;
  }
  return result;
}

/** The throughput of two steps run one after the other on the same bytes. */
function combined(a: number, b: number): number {
  return 1 / (1 / a + 1 / b);
}

const rows: Row[] = [];

for (const [name, pages] of [['text', textPages()], ['random', randomPages()]] as const) {
  const crc = throughput(() => {
    const h = new Crc64();
    for (const page of pages) h.update(page);
    h.sum64();
  });
  const lz4 = throughput(() => {
    for (const page of pages) compressBlock(page);
  });

  let crcBaseline: number | null = null;
  let lz4Baseline: number | null = null;

  if (BASELINE) {
    crcBaseline = throughput(() => {
      const h = new ReferenceCrc64();
      for (const page of pages) h.update(page);
      h.sum64();
    });
    lz4Baseline = throughput(() => {
      for (const page of pages) referenceCompressBlock(page);
    });
  }

  rows.push(row('CRC-64', name, crc, crcBaseline));
  rows.push(row('LZ4', name, lz4, lz4Baseline));
  rows.push(row('both', name, combined(crc, lz4), crcBaseline === null || lz4Baseline === null ? null : combined(crcBaseline, lz4Baseline)));
}

console.log(`${PAGES} pages of ${PAGE_SIZE} bytes, best of ${ROUNDS} rounds, Node.js ${process.version}`);
console.table(rows);

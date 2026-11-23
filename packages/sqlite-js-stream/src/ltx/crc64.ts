/**
 * CRC-64 ISO implementation, byte-compatible with Go's `hash/crc64` using
 * `crc64.MakeTable(crc64.ISO)`. This is what superfly/ltx uses for all LTX
 * checksums (page checksums, rolling DB checksum, file checksum).
 *
 * NOTE: LTX uses CRC-64 ISO, NOT ECMA. The litestream docs are misleading;
 * the actual code (`crc64.New(crc64.MakeTable(crc64.ISO))`) uses ISO.
 *
 * Algorithm matches Go's reflected `crc64.Update`:
 *   - init / xorout = all ones (0xFFFFFFFFFFFFFFFF)
 *   - polynomial register value = 0xD800000000000000
 *   - table built by right-shifting (reflected), update as
 *     `tab[byte(crc) ^ v] ^ (crc >> 8)`
 */

// The 64-bit values are kept as two unsigned 32-bit halves: BigInt arithmetic
// allocates, and one checksum is computed for every byte of every page.
const ISO_POLY_HI = 0xd8000000;
const ISO_POLY_LO = 0x00000000;

/** Builds the 256-entry CRC table for a reflected polynomial, as its two halves. */
function makeTable(polyHi: number, polyLo: number): [Uint32Array, Uint32Array] {
  const hi = new Uint32Array(256);
  const lo = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let h = 0;
    let l = i;
    for (let j = 0; j < 8; j++) {
      const carry = l & 1;
      l = ((l >>> 1) | (h << 31)) >>> 0;
      h >>>= 1;
      if (carry !== 0) {
        h = (h ^ polyHi) >>> 0;
        l = (l ^ polyLo) >>> 0;
      }
    }
    hi[i] = h;
    lo[i] = l;
  }
  return [hi, lo];
}

const [ISO_TABLE_HI, ISO_TABLE_LO] = makeTable(ISO_POLY_HI, ISO_POLY_LO);

/**
 * Crc64 is a streaming CRC-64 hasher, equivalent to Go's `hash/crc64`
 * (ISO table). Call `update()` with successive byte chunks and `sum64()` to
 * obtain the running checksum. `reset()` re-initializes the hasher.
 */
export class Crc64 {
  private hi = 0xffffffff;
  private lo = 0xffffffff;

  update(buf: Uint8Array): void {
    let hi = this.hi;
    let lo = this.lo;
    for (let i = 0; i < buf.length; i++) {
      const index = (lo ^ buf[i]) & 0xff;
      lo = (ISO_TABLE_LO[index] ^ ((lo >>> 8) | (hi << 24))) >>> 0;
      hi = (ISO_TABLE_HI[index] ^ (hi >>> 8)) >>> 0;
    }
    this.hi = hi;
    this.lo = lo;
  }

  reset(): void {
    this.hi = 0xffffffff;
    this.lo = 0xffffffff;
  }

  /** Returns the digest of all data written so far. */
  sum64(): bigint {
    return (BigInt(~this.hi >>> 0) << 32n) | BigInt(~this.lo >>> 0);
  }
}

/** Convenience: computes the CRC-64 ISO of a single buffer. */
export function crc64ISO(buf: Uint8Array): bigint {
  const h = new Crc64();
  h.update(buf);
  return h.sum64();
}

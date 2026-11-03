/**
 * Helpers shared by the S3 replica client and leaser: body reading and the
 * classification of S3 errors (404, 412, 409).
 */

/** Reads all bytes from an S3 body (stream or Buffer). */
export async function readBody(body: any): Promise<Uint8Array> {
  // AWS SDK v3 returns a mix; normalize to a Uint8Array.
  if (body instanceof Uint8Array) return body;
  if (typeof body?.on === 'function') {
    // Node stream.
    const chunks: Uint8Array[] = [];
    for await (const chunk of body as AsyncIterable<Uint8Array | string>) {
      chunks.push(typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk);
    }
    let total = 0;
    for (const c of chunks) total += c.length;
    const out = new Uint8Array(total);
    let o = 0;
    for (const c of chunks) {
      out.set(c, o);
      o += c.length;
    }
    return out;
  }
  // Web ReadableStream or Buffer.
  if (typeof body?.getReader === 'function') {
    const reader = (body as ReadableStream<Uint8Array>).getReader();
    const chunks: Uint8Array[] = [];
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    let total = 0;
    for (const c of chunks) total += c.length;
    const out = new Uint8Array(total);
    let o = 0;
    for (const c of chunks) {
      out.set(c, o);
      o += c.length;
    }
    return out;
  }
  return new Uint8Array(body as Uint8Array);
}

export function isPreconditionFailed(err: unknown): boolean {
  const code = (err as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode;
  const name = (err as { name?: string })?.name ?? '';
  return code === 412 || name === 'PreconditionFailed';
}

export function isNotFound(err: unknown): boolean {
  const code = (err as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode;
  const name = (err as { name?: string })?.name ?? '';
  return code === 404 || name === 'NoSuchKey' || name === 'NotFound';
}

/**
 * A concurrent conditional write to the same key (`409 ConditionalRequestConflict`).
 * S3 asks the caller to retry the request.
 */
export function isConditionalConflict(err: unknown): boolean {
  const code = (err as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode;
  const name = (err as { name?: string })?.name ?? '';
  return code === 409 || name === 'ConditionalRequestConflict';
}

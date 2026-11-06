/**
 * Test helper: starts a paged restore of `argv[2]` from the file replica at
 * `argv[3]`, whose background fill never gets its pages, prints "ready" and
 * waits to be killed.
 */

import { FileReplicaClient, Replica, type OpenLTXFileOptions } from '../../src/index.ts';

const backing = new FileReplicaClient(process.argv[3]);
const client = new Proxy(backing, {
  get(target, prop) {
    if (prop === 'openLtxFile') {
      return (opts: OpenLTXFileOptions) => (opts.size >= 256 * 1024 ? new Promise(() => {}) : target.openLtxFile(opts));
    }
    const value = Reflect.get(target, prop, target);
    return typeof value === 'function' ? value.bind(target) : value;
  },
});

const replica = new Replica(process.argv[2], client);
await replica.restorePaged();
process.stdout.write('ready\n');
setInterval(() => {}, 1000);

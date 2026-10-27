/**
 * @platformatic/sqlite-js-stream-pg — PostgreSQL upload strategy.
 *
 * Importing this module registers the `postgres://` and `postgresql://` schemes
 * with the core's replica URL factory, so
 * `newReplicaClientFromURL('postgres://…?prefix=app/db1')` resolves after this
 * addon is loaded.
 */

export {
  DefaultChunkSize,
  PgReplicaClient,
  newReplicaClientFromURL,
  ReplicaClientType,
  type PgReplicaClientOptions,
} from './replica-client.ts';

export { PgLeaser, type PgLeaserOptions } from './leaser.ts';

import { registerReplicaClientFactory } from '@platformatic/sqlite-js-stream';
import { newReplicaClientFromURL } from './replica-client.ts';

// Register the 'postgres' and 'postgresql' URL schemes when the addon is imported.
registerReplicaClientFactory('postgres', newReplicaClientFromURL);
registerReplicaClientFactory('postgresql', newReplicaClientFromURL);

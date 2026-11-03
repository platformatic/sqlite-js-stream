import { fencingSuite } from '../../../test/helpers/fencing-suite.ts';

import { PgReplicaClient } from '../src/index.ts';
import { connectForTest } from './helpers/db.ts';

let seq = 0;

fencingSuite(
  'pg',
  (t) => {
    const sql = connectForTest(t);
    return {
      prefix: () => `fencing/${Date.now().toString(36)}/${seq++}`,
      // Own tables, so this file never creates the tables of the other test
      // files concurrently.
      client: (prefix, generation) => new PgReplicaClient({ sql, prefix, generation, tablePrefix: 'fencing_ltx' }),
    };
  },
  { skip: process.env.PG_URL ? false : 'PG_URL is not set' },
);

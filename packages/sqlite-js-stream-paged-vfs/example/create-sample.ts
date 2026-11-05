// Creates source.db, a sample database with 500,000 users (about 20 MB).

import { rmSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

rmSync('source.db', { force: true });

const db = new DatabaseSync('source.db');
db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);

  INSERT INTO users (name, email)
    WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500000)
    SELECT 'User ' || i, 'user' || i || '@example.com' FROM n;
`);
db.close();

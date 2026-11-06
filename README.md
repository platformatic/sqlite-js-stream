# sqlite-js-stream

A **Node.js port of [Litestream](https://github.com/benbjohnson/litestream)** —
SQLite streaming replication and disaster recovery — written in **TypeScript**
and built on the built-in `node:sqlite`.

This repository is a [pnpm](https://pnpm.io) workspace. The packages live under
`packages/`:

| Package | Description |
| --- | --- |
| [`@platformatic/sqlite-js-stream`](packages/sqlite-js-stream) | The core library: SQLite streaming replication, LTX files, sync/restore, paged restores. |
| [`@platformatic/sqlite-js-stream-s3`](packages/backend-s3) | Amazon S3 upload strategy (replica client). |
| [`@platformatic/sqlite-js-stream-pg`](packages/backend-pg) | PostgreSQL upload strategy (replica client). |
| [`@platformatic/sqlite-js-stream-paged-vfs`](packages/sqlite-js-stream-paged-vfs) | A native SQLite VFS that reads database pages on demand. |

## Requirements

- **Node.js `>= 24`** — required for the built-in `node:sqlite`.
- [pnpm](https://pnpm.io) to install and run the workspace.

## Development

```bash
pnpm install       # link the workspace
pnpm build         # transpile the core package src -> dist
pnpm typecheck:all # typecheck every package
pnpm lint          # ESLint + neostandard, with a type-stripping guard
pnpm test:all      # run the core + addon test suites
pnpm test:paged-vfs # build & test the native paged VFS
```

See the [`@platformatic/sqlite-js-stream` README](packages/sqlite-js-stream/README.md)
for usage, and the architecture notes under
[`packages/sqlite-js-stream/docs`](packages/sqlite-js-stream/docs).

## License

[Apache-2.0](LICENSE).

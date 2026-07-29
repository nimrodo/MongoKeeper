# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MongoKeeper is a Rust crate (`mongokeeper`) that wraps a MongoDB collection (`TrackedCollection<T>`)
so that every update, replace, or delete automatically archives the document's previous version
into a companion `<name>_history` collection, giving a full audit trail without extra application
code.

## Commands

```sh
cargo build
cargo test --lib          # unit tests, no database needed
cargo clippy               # lint

# integration tests need a live MongoDB replica set reachable via MONGODB_URI:
docker compose up -d
export MONGODB_URI="mongodb://localhost:27017/?replicaSet=rs0"
cargo test --tests
cargo run --example basic_usage
docker compose down        # when done
```

Run a single integration test: `cargo test --tests <test_name>` (with `MONGODB_URI` set and
`docker compose up -d` running).

## Architecture

- `src/collection.rs` — `TrackedCollection<T>`, the core type. All mutating methods
  (`update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`, `bulk_write`) follow
  the same archive-then-mutate pattern: read matching documents, insert a `HistoryEntry` pre-image
  for each into the history collection, then apply the mutation — all inside one MongoDB
  transaction (`with_transaction`/`run_transaction`). `insert_one`/`insert_many` are thin
  passthroughs since a new document has no previous version to archive. Reads and anything else
  go through `.collection()` directly.
- `src/history.rs` — `HistoryEntry<T>` (archived pre-image + `archived_at` + `Operation`) and the
  `Operation` enum (`Update` | `Replace` | `Delete`).
- `src/error.rs` — thin `Error`/`Result` wrapper around `mongodb::error::Error`.
- `src/lib.rs` — public API surface and crate-level docs (also the doctest source for the README
  quick-start example).

### Transactional vs standalone mode

Every mutating method branches on `self.transactional`:
- **Transactional** (`new`/`with_history_name`, the default): archive + mutate run inside a
  single MongoDB transaction via `with_transaction`, retried automatically on transient
  transaction errors or unknown commit results for up to `RETRY_TIMEOUT` (2 minutes). Requires a
  replica set or sharded cluster — a standalone `mongod` errors on every mutating call.
- **Standalone** (`new_standalone`/`with_history_name_standalone`): archive and mutate run as two
  independent, non-transactional operations (`archive_matching_standalone`). No atomicity and no
  retry on transient errors, but a crash between archive and mutate can only ever produce a
  harmless orphaned history entry — never a mutation whose pre-image wasn't archived. Use only
  when a replica set genuinely isn't available.

When adding a new mutating method, mirror this transactional/standalone branch and add the
corresponding case to `BulkWriteModel` if the operation should be batchable.

### Bulk writes

`bulk_write` submits a `Vec<BulkWriteModel<T>>` as one MongoDB `bulkWrite` command, archiving a
pre-image for every document any update/replace/delete model matches, atomically. Requires
MongoDB server 8.0+. `BulkWriteModel::archive_target()` determines what (if anything) to archive
per model; `to_write_model()` converts to the driver's `WriteModel`.

## Requirements

Transactions require a replica set or sharded cluster. `docker-compose.yml` starts a single-node
`mongo:8` replica set (`rs0`) and auto-initiates it via a `mongo-init` sidecar — use it for local
dev and integration tests rather than configuring MongoDB manually.

# MongoKeeper Design

## Context

MongoKeeper is a Rust library that wraps MongoDB collections to automatically
preserve historical versions of documents. Whenever a document in a tracked
collection is updated, replaced, or deleted, the library archives the
pre-mutation version of the document into a companion history collection
before applying the mutation. This gives applications an audit trail /
change history for any collection without requiring them to hand-roll
read-then-write archival logic on every mutation path.

The document shape is generic (any `Serialize`/`Deserialize` type), so the
library can be reused across different collections and schemas in a project.

## Goals

- Provide a near drop-in wrapper around `mongodb::Collection<T>` that
  transparently archives pre-images on update/replace/delete.
- Guarantee no lost or duplicated history entries via MongoDB transactions.
- Ship with tests, a runnable example, and thorough documentation.

## Non-goals

- Diffing/patch-based history (full snapshots only).
- Automatic history via change streams (explicitly rejected in favor of a
  wrapper API — deterministic, no replica-set-only change-stream lag, works
  the moment the call returns).
- Supporting standalone (non-replica-set) MongoDB deployments.

## Architecture

### Crate: `mongokeeper`

Core type: `TrackedCollection<T>`, generic over
`T: Serialize + DeserializeOwned + Clone + Unpin + Send + Sync`.

```rust
pub struct TrackedCollection<T> {
    collection: mongodb::Collection<T>,
    history: mongodb::Collection<HistoryEntry<T>>,
}
```

Construction:

```rust
impl<T> TrackedCollection<T> {
    /// History collection name defaults to "<collection>_history".
    pub fn new(db: &Database, collection_name: &str) -> Self;

    /// Explicit history collection name / suffix override.
    pub fn with_history_name(db: &Database, collection_name: &str, history_name: &str) -> Self;
}
```

### History document shape

```rust
#[derive(Serialize, Deserialize)]
pub struct HistoryEntry<T> {
    pub archived_at: bson::DateTime,
    pub operation: Operation,
    pub document: T,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Update,
    Replace,
    Delete,
}
```

### Mutating operations

Each of these opens a client session, starts a transaction, reads the
document(s) currently matching the filter, inserts a `HistoryEntry` pre-image
for each into the history collection, applies the mutation, and commits the
transaction. On any failure the transaction is aborted and the error is
returned — no partial archive, no partial mutation.

- `update_one(filter, update) -> UpdateResult`
- `update_many(filter, update) -> UpdateResult`
- `replace_one(filter, replacement: T) -> UpdateResult`
- `delete_one(filter) -> DeleteResult`
- `delete_many(filter) -> DeleteResult`

For `update_many`/`delete_many`, every document matching the filter at
read time is archived (one `HistoryEntry` per document) before the bulk
mutation is applied, all within the same transaction.

Signatures mirror the underlying `mongodb` crate's `Collection<T>` methods
(same filter/update/options types) so switching from `Collection<T>` to
`TrackedCollection<T>` requires minimal call-site changes.

### Read passthrough

Non-mutating access is provided via:

```rust
impl<T> TrackedCollection<T> {
    pub fn collection(&self) -> &mongodb::Collection<T>;
    pub fn history(&self) -> &mongodb::Collection<HistoryEntry<T>>;
}
```

Callers use `.collection()` for `find`/`find_one`/aggregation etc., and
`.history()` to query historical entries directly with the full `mongodb`
API (e.g. filter by `operation`, sort by `archived_at`).

### Transactions requirement

MongoDB transactions require a replica set (a local single-node replica set
is sufficient for development). This is a hard requirement, documented
prominently in the crate root doc comment and the README, including
instructions for initializing a local single-node RS for dev/test.

### Error handling

```rust
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}
```

Driver errors (including transient/retryable transaction errors, per
MongoDB's transaction retry semantics) propagate directly to the caller
without being wrapped or swallowed. No custom retry loop is implemented —
callers can inspect `mongodb::error::Error`'s labels (e.g.
`TransientTransactionError`) if they want to retry.

## Testing

Integration tests (`tests/`) run against a real MongoDB replica set,
reachable via a `MONGODB_URI` env var (documented in README; a
docker-compose or single-node RS setup snippet provided). Tests cover:

- `update_one` archives exactly one pre-image with `Operation::Update`.
- `update_many` archives one pre-image per matched document.
- `replace_one` archives the pre-replacement document with `Operation::Replace`.
- `delete_one` / `delete_many` archive pre-delete documents with `Operation::Delete`.
- History collection defaults to `<name>_history`; `with_history_name`
  override works.
- A failure injected mid-transaction (e.g. a duplicate-key error on the
  mutation step) leaves neither a new history entry nor a mutated document
  (rollback verified).

Unit tests (in-module, no DB) cover `HistoryEntry`/`Operation`
(de)serialization round-trips.

## Example

`examples/basic_usage.rs`: models a simple `Order { _id, customer, total,
status }` struct, connects to MongoDB, wraps it in a `TrackedCollection`,
performs an update and a delete, then reads back and prints the history
entries.

## Documentation

- Crate-level `//!` doc comment: what the library does, the replica-set
  requirement, a quick-start snippet.
- Rustdoc on `TrackedCollection`, `HistoryEntry`, `Operation`, and each
  method, with examples.
- README: install instructions, local replica-set setup for dev/testing,
  quick-start example, testing instructions.

## Project layout

```
Cargo.toml
src/
  lib.rs
  collection.rs   // TrackedCollection<T> + mutating ops
  history.rs      // HistoryEntry<T>, Operation
  error.rs        // Error
tests/
  integration_test.rs
examples/
  basic_usage.rs
README.md
```

## Dependencies

- `mongodb` (official async driver, tokio runtime)
- `serde`, `bson`
- `thiserror`
- `tokio` (dev-dependency for tests/examples, `full` or `rt-multi-thread` + `macros`)

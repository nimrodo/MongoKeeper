# MongoKeeper

MongoKeeper wraps a [MongoDB](https://www.mongodb.com/) collection so that every update,
replace, or delete automatically archives the document's previous version into a companion
history collection — giving you a full audit trail with no extra application code.

```rust
let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");

// Archives the current "orders" document into "orders_history" first, then applies the update.
orders
    .update_one(doc! { "status": "pending" }, doc! { "$set": { "status": "shipped" } })
    .await?;
```

## How it works

`TrackedCollection<T>` wraps an `mongodb::Collection<T>` plus a derived history collection
(`<name>_history` by default). Each mutating call — `update_one`, `update_many`,
`replace_one`, `delete_one`, `delete_many` — runs inside a single MongoDB transaction:

1. Read every document currently matching the filter.
2. Insert one archived pre-image per document into the history collection.
3. Apply the mutation.
4. Commit.

If any step fails, the transaction is aborted: no document is archived without its mutation
succeeding, and no mutation succeeds without its previous version being archived.

`insert_one`/`insert_many` are also available directly on `TrackedCollection` — thin
passthroughs with nothing to archive, since a newly-inserted document has no previous version.

Reads (`find`, `find_one`, etc.) and any other operation not covered by this type pass straight
through via `.collection()`. Query the history directly via `.history()`.

Transactions that fail with a transient error (a write conflict, a replica set election, etc.)
are retried automatically, following the retry pattern recommended by MongoDB's own
transactions documentation. Retries continue for up to two minutes before the error is
returned to the caller.

## Bulk writes

`bulk_write` submits a batch of insert/update/replace/delete operations as a single MongoDB
`bulkWrite` command, archiving a pre-image for every document any update/replace/delete model
in the batch matches — all atomically, in one transaction:

```rust
use mongokeeper::BulkWriteModel;

orders
    .bulk_write(vec![
        BulkWriteModel::UpdateMany {
            filter: doc! { "status": "pending" },
            update: doc! { "$set": { "status": "shipped" } },
        },
        BulkWriteModel::DeleteOne {
            filter: doc! { "status": "cancelled" },
        },
    ])
    .await?;
```

`bulk_write` requires **MongoDB server 8.0 or later**, in addition to the replica-set
requirement below.

## Requirements

**This crate requires MongoDB transactions, which require a replica set or sharded
cluster.** A standalone `mongod` will return an error on every mutating call.

To run a single-node replica set locally for development or testing, either use the bundled
`docker-compose.yml` (recommended — starts MongoDB and initializes the replica set for you):

```sh
docker compose up -d
```

or start one manually:

```sh
mongod --replSet rs0 --dbpath /path/to/data --port 27017
# in another shell, one-time setup:
mongosh --port 27017 --eval "rs.initiate()"
```

Either way, point the driver at it with a `replicaSet` connection string parameter, e.g.
`mongodb://localhost:27017/?replicaSet=rs0`.

### Standalone `mongod` (no replica set)

If a replica set genuinely isn't available, `TrackedCollection::new_standalone` /
`with_history_name_standalone` construct a collection that never uses transactions:

```rust
let orders: TrackedCollection<Order> = TrackedCollection::new_standalone(&db, "orders");
```

This trades away atomicity: archiving and mutating become two independent operations instead
of one, so a crash between them (or a partial failure archiving multiple documents for
`update_many`/`delete_many`/`bulk_write`) can leave a harmless orphaned history entry — a
pre-image was archived but the corresponding mutation never happened, so the entry just
duplicates the document's current state. It can never produce a mutation whose pre-image was
never archived. There's also no automatic retry on transient errors in this mode. Prefer the
transactional constructors whenever a replica set is available.

## Usage

```rust
use bson::{doc, oid::ObjectId};
use mongodb::Client;
use mongokeeper::TrackedCollection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    #[serde(rename = "_id")]
    id: ObjectId,
    status: String,
}

let client = Client::with_uri_str("mongodb://localhost:27017/?replicaSet=rs0").await?;
let db = client.database("shop");

// History is stored in "orders_history" by default.
let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");
// Or with an explicit history collection name:
// let orders = TrackedCollection::with_history_name(&db, "orders", "orders_archive");

orders
    .update_one(doc! { "status": "pending" }, doc! { "$set": { "status": "shipped" } })
    .await?;

// Query archived versions directly.
let mut history = orders.history().find(doc! {}).await?;
```

See `examples/basic_usage.rs` for a complete runnable example.

## History pruning

History collections grow unboundedly by default. Two ways to bound them:

```rust
use std::time::Duration;

// Set-and-forget: MongoDB automatically deletes entries older than 30 days. Safe to call on
// every startup. Deletion is best-effort — MongoDB sweeps for expired documents roughly once
// every 60 seconds, so entries may briefly outlive this window.
orders
    .ensure_history_ttl_index(Duration::from_secs(30 * 24 * 3600))
    .await?;

// On-demand: deletes matching entries immediately and returns how many were removed.
let deleted = orders.prune_history_older_than(Duration::from_secs(30 * 24 * 3600)).await?;
```

## History document shape

```rust
pub struct HistoryEntry<T> {
    pub archived_at: bson::DateTime,
    pub operation: Operation, // Update | Replace | Delete
    pub document: T,          // full pre-mutation document
}
```

## Development

```sh
cargo build
cargo test --lib          # unit tests, no database needed

# integration tests need a replica set reachable via MONGODB_URI:
docker compose up -d
export MONGODB_URI="mongodb://localhost:27017/?replicaSet=rs0"
cargo test --tests
cargo run --example basic_usage

docker compose down        # when you're done
```

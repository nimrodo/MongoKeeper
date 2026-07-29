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

Reads (`find`, `find_one`, etc.) and any operation not listed above pass straight through via
`.collection()`. Query the history directly via `.history()`.

## Requirements

**This crate requires MongoDB transactions, which require a replica set or sharded
cluster.** A standalone `mongod` will return an error on every mutating call.

To run a single-node replica set locally for development or testing:

```sh
mongod --replSet rs0 --dbpath /path/to/data --port 27017
# in another shell, one-time setup:
mongosh --port 27017 --eval "rs.initiate()"
```

Then point the driver at it with a `replicaSet` connection string parameter, e.g.
`mongodb://localhost:27017/?replicaSet=rs0`.

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
export MONGODB_URI="mongodb://localhost:27117/?replicaSet=rs0"
cargo test --tests
cargo run --example basic_usage
```

//! MongoKeeper wraps a MongoDB collection so that every update, replace, or delete
//! automatically archives the document's previous version into a companion history
//! collection — giving you a full audit trail with no extra application code.
//!
//! # Requirements
//!
//! This crate uses MongoDB multi-document transactions to guarantee that a document is
//! never mutated without its previous version being archived (and vice versa). Transactions
//! require a replica set or sharded cluster; a standalone `mongod` will return an error. See
//! the README for how to run a single-node replica set locally for development and testing.
//!
//! # Quick start
//!
//! ```no_run
//! use mongodb::Client;
//! use mongokeeper::TrackedCollection;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Order {
//!     #[serde(rename = "_id")]
//!     id: bson::oid::ObjectId,
//!     status: String,
//! }
//!
//! # async fn run() -> mongokeeper::Result<()> {
//! let client = Client::with_uri_str("mongodb://localhost:27017").await?;
//! let db = client.database("shop");
//!
//! // History is stored in "orders_history" by default.
//! let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");
//!
//! orders
//!     .update_one(
//!         bson::doc! { "status": "pending" },
//!         bson::doc! { "$set": { "status": "shipped" } },
//!     )
//!     .await?;
//!
//! // Query the archived pre-images directly.
//! let mut history = orders.history().find(bson::doc! {}).await?;
//! # Ok(())
//! # }
//! ```

mod collection;
mod error;
mod history;

pub use collection::TrackedCollection;
pub use error::{Error, Result};
pub use history::{HistoryEntry, Operation};

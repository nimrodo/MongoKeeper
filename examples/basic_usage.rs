//! Demonstrates wrapping a collection with `TrackedCollection`, mutating documents, and
//! inspecting the archived history that results.
//!
//! Requires a MongoDB replica set reachable via the `MONGODB_URI` env var
//! (defaults to `mongodb://localhost:27017`). See the README for how to set one up locally.

use bson::{doc, oid::ObjectId};
use futures_util::TryStreamExt;
use mongodb::Client;
use mongokeeper::TrackedCollection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    #[serde(rename = "_id")]
    id: ObjectId,
    customer: String,
    total: i32,
    status: String,
}

#[tokio::main]
async fn main() -> mongokeeper::Result<()> {
    let uri =
        std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    let client = Client::with_uri_str(&uri).await?;
    let db = client.database("mongokeeper_example");

    // History is archived automatically into "orders_history".
    let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");

    let order = Order {
        id: ObjectId::new(),
        customer: "Ada Lovelace".to_string(),
        total: 42,
        status: "pending".to_string(),
    };
    orders.collection().insert_one(order.clone()).await?;
    println!("Inserted order {}", order.id);

    orders
        .update_one(
            doc! { "_id": order.id },
            doc! { "$set": { "status": "shipped" } },
        )
        .await?;
    println!("Updated order status to \"shipped\"");

    orders.delete_one(doc! { "_id": order.id }).await?;
    println!("Deleted order {}", order.id);

    let history: Vec<_> = orders
        .history()
        .find(doc! { "document._id": order.id })
        .await?
        .try_collect()
        .await?;

    println!("\nHistory for order {}:", order.id);
    for entry in history {
        println!(
            "  [{:?}] archived_at={} status={}",
            entry.operation, entry.archived_at, entry.document.status
        );
    }

    client.shutdown().await;
    Ok(())
}

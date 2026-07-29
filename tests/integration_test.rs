//! Integration tests against a real MongoDB replica set.
//!
//! Set `MONGODB_URI` to point at a replica set (transactions require one; a standalone
//! `mongod` will fail). See the README for how to run one locally. Each test uses a
//! uniquely-named database (dropped at the start of the test) so tests can run concurrently
//! without interfering and remain independent of leftover data from previous runs.

use bson::{doc, oid::ObjectId};
use futures_util::TryStreamExt;
use mongodb::{Client, Database};
use mongokeeper::{BulkWriteModel, Operation, TrackedCollection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Widget {
    #[serde(rename = "_id")]
    id: ObjectId,
    name: String,
    count: i32,
}

async fn test_db(name: &str) -> Database {
    let uri =
        std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    let client = Client::with_uri_str(&uri)
        .await
        .expect("connect to MongoDB (requires a reachable replica set)");
    let db = client.database(&format!("mongokeeper_test_{name}"));
    // Each test uses a fixed database name; drop any leftover data from a previous run so
    // tests stay independent of run history.
    db.drop().await.expect("drop stale test database");
    db
}

#[tokio::test]
async fn update_one_archives_pre_image() {
    let db = test_db("update_one").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "sprocket".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    widgets
        .update_one(doc! { "_id": id }, doc! { "$set": { "count": 2 } })
        .await
        .unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! { "document._id": id })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation, Operation::Update);
    assert_eq!(history[0].document.count, 1);

    let current = widgets
        .collection()
        .find_one(doc! { "_id": id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.count, 2);
}

#[tokio::test]
async fn update_many_archives_one_entry_per_matched_document() {
    let db = test_db("update_many").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let ids = [ObjectId::new(), ObjectId::new(), ObjectId::new()];
    for id in ids {
        widgets
            .collection()
            .insert_one(Widget {
                id,
                name: "gear".to_string(),
                count: 1,
            })
            .await
            .unwrap();
    }

    widgets
        .update_many(
            doc! { "name": "gear" },
            doc! { "$set": { "count": 5 } },
        )
        .await
        .unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(history.len(), 3);
    assert!(history.iter().all(|e| e.operation == Operation::Update));
}

#[tokio::test]
async fn replace_one_archives_pre_replacement_document() {
    let db = test_db("replace_one").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "bolt".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    widgets
        .replace_one(
            doc! { "_id": id },
            Widget {
                id,
                name: "bolt-v2".to_string(),
                count: 10,
            },
        )
        .await
        .unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! { "document._id": id })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation, Operation::Replace);
    assert_eq!(history[0].document.name, "bolt");

    let current = widgets
        .collection()
        .find_one(doc! { "_id": id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.name, "bolt-v2");
}

#[tokio::test]
async fn delete_one_archives_pre_delete_document() {
    let db = test_db("delete_one").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "nut".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    widgets.delete_one(doc! { "_id": id }).await.unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! { "document._id": id })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation, Operation::Delete);

    let current = widgets
        .collection()
        .find_one(doc! { "_id": id })
        .await
        .unwrap();
    assert!(current.is_none());
}

#[tokio::test]
async fn delete_many_archives_one_entry_per_matched_document() {
    let db = test_db("delete_many").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    for _ in 0..3 {
        widgets
            .collection()
            .insert_one(Widget {
                id: ObjectId::new(),
                name: "washer".to_string(),
                count: 1,
            })
            .await
            .unwrap();
    }

    widgets
        .delete_many(doc! { "name": "washer" })
        .await
        .unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(history.len(), 3);
    assert!(history.iter().all(|e| e.operation == Operation::Delete));

    let remaining = widgets
        .collection()
        .count_documents(doc! {})
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn concurrent_update_one_retries_transient_conflicts_and_archives_exactly_once_each() {
    let db = test_db("concurrent_update").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "cog".to_string(),
            count: 0,
        })
        .await
        .unwrap();

    // Two concurrent update_one calls against the same document provoke a write conflict:
    // whichever transaction loses the conflict fails with a TransientTransactionError, which
    // TrackedCollection retries automatically until it succeeds.
    let (r1, r2) = tokio::join!(
        widgets.update_one(doc! { "_id": id }, doc! { "$set": { "count": 1 } }),
        widgets.update_one(doc! { "_id": id }, doc! { "$set": { "count": 2 } }),
    );
    r1.unwrap();
    r2.unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! { "document._id": id })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // Exactly one archived pre-image per successful update, no extras from aborted/retried
    // attempts (their archive inserts were rolled back along with the rest of the transaction).
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|e| e.operation == Operation::Update));
}

#[tokio::test]
async fn default_history_collection_name_is_suffixed() {
    let db = test_db("naming").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    assert_eq!(widgets.history().name(), "widgets_history");
}

#[tokio::test]
async fn custom_history_collection_name_is_used() {
    let db = test_db("naming_custom").await;
    let widgets: TrackedCollection<Widget> =
        TrackedCollection::with_history_name(&db, "widgets", "widgets_archive");
    assert_eq!(widgets.history().name(), "widgets_archive");
}

#[tokio::test]
async fn failed_mutation_leaves_no_history_and_no_change() {
    let db = test_db("rollback").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "rivet".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    // An invalid replacement (mismatched `_id` between filter and replacement in a way
    // that MongoDB rejects) forces the mutation step to fail after the archive step has
    // already run; the transaction should still roll back the archive.
    let other_id = ObjectId::new();
    let result = widgets
        .replace_one(
            doc! { "_id": id },
            Widget {
                id: other_id,
                name: "rivet-v2".to_string(),
                count: 2,
            },
        )
        .await;
    assert!(result.is_err());

    let history_count = widgets
        .history()
        .count_documents(doc! { "document._id": id })
        .await
        .unwrap();
    assert_eq!(history_count, 0);

    let current = widgets
        .collection()
        .find_one(doc! { "_id": id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.name, "rivet");
}

#[tokio::test]
async fn bulk_write_mixed_operations_archives_correctly() {
    let db = test_db("bulk_write_mixed").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let update_id = ObjectId::new();
    let delete_id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id: update_id,
            name: "clamp".to_string(),
            count: 1,
        })
        .await
        .unwrap();
    widgets
        .collection()
        .insert_one(Widget {
            id: delete_id,
            name: "hinge".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    let insert_id = ObjectId::new();
    let summary = widgets
        .bulk_write(vec![
            BulkWriteModel::InsertOne(Widget {
                id: insert_id,
                name: "pin".to_string(),
                count: 1,
            }),
            BulkWriteModel::UpdateOne {
                filter: doc! { "_id": update_id },
                update: doc! { "$set": { "count": 2 } },
            },
            BulkWriteModel::DeleteOne {
                filter: doc! { "_id": delete_id },
            },
        ])
        .await
        .unwrap();

    assert_eq!(summary.inserted_count, 1);
    assert_eq!(summary.modified_count, 1);
    assert_eq!(summary.deleted_count, 1);

    let history: Vec<_> = widgets
        .history()
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert!(
        history
            .iter()
            .any(|e| e.document.id == update_id && e.operation == Operation::Update)
    );
    assert!(
        history
            .iter()
            .any(|e| e.document.id == delete_id && e.operation == Operation::Delete)
    );

    assert!(
        widgets
            .collection()
            .find_one(doc! { "_id": insert_id })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        widgets
            .collection()
            .find_one(doc! { "_id": delete_id })
            .await
            .unwrap()
            .is_none()
    );
    let updated = widgets
        .collection()
        .find_one(doc! { "_id": update_id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.count, 2);
}

#[tokio::test]
async fn bulk_write_many_variants_archive_one_entry_per_document() {
    let db = test_db("bulk_write_many").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    for _ in 0..3 {
        widgets
            .collection()
            .insert_one(Widget {
                id: ObjectId::new(),
                name: "spring".to_string(),
                count: 1,
            })
            .await
            .unwrap();
    }
    for _ in 0..2 {
        widgets
            .collection()
            .insert_one(Widget {
                id: ObjectId::new(),
                name: "rod".to_string(),
                count: 1,
            })
            .await
            .unwrap();
    }

    widgets
        .bulk_write(vec![
            BulkWriteModel::UpdateMany {
                filter: doc! { "name": "spring" },
                update: doc! { "$set": { "count": 9 } },
            },
            BulkWriteModel::DeleteMany {
                filter: doc! { "name": "rod" },
            },
        ])
        .await
        .unwrap();

    let history: Vec<_> = widgets
        .history()
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(history.len(), 5);
    assert_eq!(
        history
            .iter()
            .filter(|e| e.operation == Operation::Update)
            .count(),
        3
    );
    assert_eq!(
        history
            .iter()
            .filter(|e| e.operation == Operation::Delete)
            .count(),
        2
    );
}

#[tokio::test]
async fn bulk_write_failure_leaves_no_history_and_no_change() {
    let db = test_db("bulk_write_rollback").await;
    let widgets: TrackedCollection<Widget> = TrackedCollection::new(&db, "widgets");
    let id = ObjectId::new();
    widgets
        .collection()
        .insert_one(Widget {
            id,
            name: "washer".to_string(),
            count: 1,
        })
        .await
        .unwrap();

    // An empty update document is rejected by MongoDB ("no operations in update"), so the
    // whole bulk write (and its transaction) should fail and roll back.
    let result = widgets
        .bulk_write(vec![BulkWriteModel::UpdateOne {
            filter: doc! { "_id": id },
            update: doc! {},
        }])
        .await;
    assert!(result.is_err());

    let history_count = widgets
        .history()
        .count_documents(doc! { "document._id": id })
        .await
        .unwrap();
    assert_eq!(history_count, 0);

    let current = widgets
        .collection()
        .find_one(doc! { "_id": id })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.count, 1);
}

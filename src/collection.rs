//! [`TrackedCollection`], a MongoDB collection wrapper that archives previous document
//! versions on update, replace, and delete.

use bson::Document;
use mongodb::results::{DeleteResult, UpdateResult};
use mongodb::{Client, ClientSession, Collection, Database};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::Result;
use crate::history::{HistoryEntry, Operation};

/// A MongoDB collection wrapper that transparently archives the previous version of any
/// document affected by an update, replace, or delete into a companion history collection.
///
/// Every mutating operation runs inside a MongoDB transaction: the affected document(s) are
/// read and archived, then the mutation is applied, then the transaction is committed. If any
/// step fails, the transaction is aborted and neither the archive nor the mutation take effect.
///
/// # Requirements
///
/// Transactions require MongoDB to be running as a replica set (or sharded cluster) — a
/// standalone `mongod` does not support them. For local development, initialize a
/// single-node replica set (see the crate README).
///
/// # Example
///
/// ```no_run
/// use mongodb::Client;
/// use mongokeeper::TrackedCollection;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct Order {
///     #[serde(rename = "_id")]
///     id: bson::oid::ObjectId,
///     status: String,
/// }
///
/// # async fn run() -> mongokeeper::Result<()> {
/// let client = Client::with_uri_str("mongodb://localhost:27017").await?;
/// let db = client.database("shop");
/// let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");
///
/// orders
///     .update_one(bson::doc! { "_id": bson::oid::ObjectId::new() }, bson::doc! { "$set": { "status": "shipped" } })
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct TrackedCollection<T: Send + Sync> {
    client: Client,
    collection: Collection<T>,
    history: Collection<HistoryEntry<T>>,
}

impl<T> TrackedCollection<T>
where
    T: Serialize + DeserializeOwned + Clone + Unpin + Send + Sync,
{
    /// Wraps `collection_name` in `db`, using the default history collection name
    /// (`"<collection_name>_history"`).
    pub fn new(db: &Database, collection_name: &str) -> Self {
        let history_name = format!("{collection_name}_history");
        Self::with_history_name(db, collection_name, &history_name)
    }

    /// Wraps `collection_name` in `db`, storing history in `history_name` instead of the
    /// default `"<collection_name>_history"`.
    pub fn with_history_name(db: &Database, collection_name: &str, history_name: &str) -> Self {
        Self {
            client: db.client().clone(),
            collection: db.collection(collection_name),
            history: db.collection(history_name),
        }
    }

    /// The wrapped main collection, for reads and any operation not covered by this type
    /// (e.g. `find`, `find_one`, `insert_one`, aggregation).
    pub fn collection(&self) -> &Collection<T> {
        &self.collection
    }

    /// The history collection that archived versions are stored in.
    pub fn history(&self) -> &Collection<HistoryEntry<T>> {
        &self.history
    }

    /// Archives the current version of every document matching `filter`, then applies
    /// `update` to it, atomically.
    pub async fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        let mut session = self.begin(filter.clone(), Operation::Update).await?;
        let outcome = self
            .collection
            .update_one(filter, update)
            .session(&mut session)
            .await;
        self.finish(session, outcome).await
    }

    /// Archives the current version of every document matching `filter`, then applies
    /// `update` to all of them, atomically.
    pub async fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        let mut session = self.begin(filter.clone(), Operation::Update).await?;
        let outcome = self
            .collection
            .update_many(filter, update)
            .session(&mut session)
            .await;
        self.finish(session, outcome).await
    }

    /// Archives the current version of the document matching `filter`, then replaces it
    /// with `replacement`, atomically.
    pub async fn replace_one(&self, filter: Document, replacement: T) -> Result<UpdateResult> {
        let mut session = self.begin(filter.clone(), Operation::Replace).await?;
        let outcome = self
            .collection
            .replace_one(filter, replacement)
            .session(&mut session)
            .await;
        self.finish(session, outcome).await
    }

    /// Archives the current version of the document matching `filter`, then deletes it,
    /// atomically.
    pub async fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        let mut session = self.begin(filter.clone(), Operation::Delete).await?;
        let outcome = self.collection.delete_one(filter).session(&mut session).await;
        self.finish(session, outcome).await
    }

    /// Archives the current version of every document matching `filter`, then deletes all
    /// of them, atomically.
    pub async fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        let mut session = self.begin(filter.clone(), Operation::Delete).await?;
        let outcome = self.collection.delete_many(filter).session(&mut session).await;
        self.finish(session, outcome).await
    }

    /// Starts a transaction and archives the current version of every document matching
    /// `filter`. On failure, aborts the transaction before returning the error.
    async fn begin(&self, filter: Document, operation: Operation) -> Result<ClientSession> {
        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;

        if let Err(err) = self.archive_matching(&mut session, filter, operation).await {
            session.abort_transaction().await?;
            return Err(err.into());
        }

        Ok(session)
    }

    /// Commits the transaction if `outcome` is `Ok`, otherwise aborts it; either way,
    /// returns `outcome` converted to this crate's `Result`.
    async fn finish<R>(
        &self,
        mut session: ClientSession,
        outcome: mongodb::error::Result<R>,
    ) -> Result<R> {
        match outcome {
            Ok(result) => {
                session.commit_transaction().await?;
                Ok(result)
            }
            Err(err) => {
                session.abort_transaction().await?;
                Err(err.into())
            }
        }
    }

    async fn archive_matching(
        &self,
        session: &mut ClientSession,
        filter: Document,
        operation: Operation,
    ) -> mongodb::error::Result<()> {
        let mut cursor = self.collection.find(filter).session(&mut *session).await?;
        let mut entries = Vec::new();
        while let Some(document) = cursor.next(session).await.transpose()? {
            entries.push(HistoryEntry {
                archived_at: bson::DateTime::now(),
                operation,
                document,
            });
        }

        if !entries.is_empty() {
            self.history.insert_many(entries).session(session).await?;
        }

        Ok(())
    }
}

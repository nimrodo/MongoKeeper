//! [`TrackedCollection`], a MongoDB collection wrapper that archives previous document
//! versions on update, replace, and delete.

use std::time::{Duration, Instant};

use bson::Document;
use mongodb::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};
use mongodb::results::{DeleteResult, UpdateResult};
use mongodb::{Client, ClientSession, Collection, Database};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::Result;
use crate::history::{HistoryEntry, Operation};

/// How long to keep retrying a transaction that keeps failing with a transient error, or a
/// commit whose result is unknown, before giving up. Matches the retry window recommended in
/// MongoDB's own transactions documentation.
const RETRY_TIMEOUT: Duration = Duration::from_secs(120);

/// A MongoDB collection wrapper that transparently archives the previous version of any
/// document affected by an update, replace, or delete into a companion history collection.
///
/// Every mutating operation runs inside a MongoDB transaction: the affected document(s) are
/// read and archived, then the mutation is applied, then the transaction is committed. If any
/// step fails, the transaction is aborted and neither the archive nor the mutation take effect.
///
/// Transactions that fail with a transient error (e.g. a write conflict, or a replica set
/// election) are retried automatically, as are commits whose outcome is unknown, following
/// the retry pattern recommended by MongoDB's transactions documentation. Retries continue for
/// up to two minutes before the error is returned to the caller.
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
        self.run_transaction(filter, Operation::Update, async |session, filter| {
            self.collection
                .update_one(filter, update.clone())
                .session(session)
                .await
        })
        .await
    }

    /// Archives the current version of every document matching `filter`, then applies
    /// `update` to all of them, atomically.
    pub async fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.run_transaction(filter, Operation::Update, async |session, filter| {
            self.collection
                .update_many(filter, update.clone())
                .session(session)
                .await
        })
        .await
    }

    /// Archives the current version of the document matching `filter`, then replaces it
    /// with `replacement`, atomically.
    pub async fn replace_one(&self, filter: Document, replacement: T) -> Result<UpdateResult> {
        self.run_transaction(filter, Operation::Replace, async |session, filter| {
            self.collection
                .replace_one(filter, replacement.clone())
                .session(session)
                .await
        })
        .await
    }

    /// Archives the current version of the document matching `filter`, then deletes it,
    /// atomically.
    pub async fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        self.run_transaction(filter, Operation::Delete, async |session, filter| {
            self.collection.delete_one(filter).session(session).await
        })
        .await
    }

    /// Archives the current version of every document matching `filter`, then deletes all
    /// of them, atomically.
    pub async fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        self.run_transaction(filter, Operation::Delete, async |session, filter| {
            self.collection.delete_many(filter).session(session).await
        })
        .await
    }

    /// Runs the archive-then-mutate pattern shared by every mutating method inside a
    /// transaction: read every document matching `filter`, insert a [`HistoryEntry`] for each
    /// into the history collection, then run `mutate`, then commit.
    ///
    /// If the transaction fails with a transient error (a write conflict, a replica set
    /// election, etc.), the whole attempt — archive included — is retried from scratch, since
    /// the transaction's read snapshot no longer applies. Retries continue for up to
    /// [`RETRY_TIMEOUT`] before the error is returned to the caller.
    async fn run_transaction<R>(
        &self,
        filter: Document,
        operation: Operation,
        mut mutate: impl AsyncFnMut(&mut ClientSession, Document) -> mongodb::error::Result<R>,
    ) -> Result<R> {
        let deadline = Instant::now() + RETRY_TIMEOUT;

        loop {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;

            let outcome = match self
                .archive_matching(&mut session, filter.clone(), operation)
                .await
            {
                Ok(()) => mutate(&mut session, filter.clone()).await,
                Err(err) => Err(err),
            };

            match outcome {
                Ok(value) => match self.commit(&mut session).await {
                    Ok(()) => return Ok(value),
                    Err(err) => return Err(err.into()),
                },
                Err(err) if is_transient_transaction_error(&err) && Instant::now() < deadline => {
                    let _ = session.abort_transaction().await;
                    continue;
                }
                Err(err) => {
                    session.abort_transaction().await?;
                    return Err(err.into());
                }
            }
        }
    }

    /// Commits `session`'s transaction, retrying if the outcome is unknown (e.g. after a
    /// network blip during the commit itself) for up to [`RETRY_TIMEOUT`].
    async fn commit(&self, session: &mut ClientSession) -> mongodb::error::Result<()> {
        let deadline = Instant::now() + RETRY_TIMEOUT;
        loop {
            match session.commit_transaction().await {
                Ok(()) => return Ok(()),
                Err(err) if is_unknown_commit_result(&err) && Instant::now() < deadline => {
                    continue;
                }
                Err(err) => return Err(err),
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

fn is_transient_transaction_error(err: &mongodb::error::Error) -> bool {
    err.contains_label(TRANSIENT_TRANSACTION_ERROR)
}

fn is_unknown_commit_result(err: &mongodb::error::Error) -> bool {
    err.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
}

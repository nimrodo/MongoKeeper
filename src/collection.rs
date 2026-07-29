//! [`TrackedCollection`], a MongoDB collection wrapper that archives previous document
//! versions on update, replace, and delete.

use std::borrow::Borrow;
use std::time::{Duration, Instant, SystemTime};

use bson::{Document, doc};
use futures_util::TryStreamExt;
use mongodb::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};
use mongodb::options::{
    DeleteManyModel, DeleteOneModel, IndexOptions, UpdateManyModel, UpdateModifications,
    UpdateOneModel, WriteModel,
};
use mongodb::results::{
    DeleteResult, InsertManyResult, InsertOneResult, SummaryBulkWriteResult, UpdateResult,
};
use mongodb::{Client, ClientSession, Collection, Database, IndexModel};
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
/// # Non-transactional mode
///
/// [`new_standalone`](Self::new_standalone) and
/// [`with_history_name_standalone`](Self::with_history_name_standalone) construct a
/// `TrackedCollection` that never uses transactions, for standalone `mongod` deployments that
/// can't support them. This trades away atomicity: archiving and mutating become two
/// independent operations instead of one, so a crash between them (or a partial failure
/// archiving multiple documents for `update_many`/`delete_many`/`bulk_write`) can leave a
/// harmless orphaned history entry — a pre-image was archived but the corresponding mutation
/// never happened, so the archived entry simply duplicates the document's current state. It
/// can never produce a mutation whose pre-image was never archived. There is also no retry on
/// transient errors in this mode, since the transaction-specific error labels this crate
/// retries on don't apply outside a transaction.
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
    transactional: bool,
}

/// A single operation to submit as part of a [`TrackedCollection::bulk_write`] call, scoped to
/// the collection being wrapped.
pub enum BulkWriteModel<T> {
    /// Inserts `T`. Nothing is archived, since there is no previous version.
    InsertOne(T),
    /// Archives the current version of the first document matching `filter`, then updates it.
    UpdateOne {
        /// The filter selecting which document to update.
        filter: Document,
        /// The update to apply.
        update: Document,
    },
    /// Archives the current version of every document matching `filter`, then updates all of
    /// them.
    UpdateMany {
        /// The filter selecting which documents to update.
        filter: Document,
        /// The update to apply.
        update: Document,
    },
    /// Archives the current version of the first document matching `filter`, then replaces it.
    ReplaceOne {
        /// The filter selecting which document to replace.
        filter: Document,
        /// The document to replace it with.
        replacement: T,
    },
    /// Archives the current version of the first document matching `filter`, then deletes it.
    DeleteOne {
        /// The filter selecting which document to delete.
        filter: Document,
    },
    /// Archives the current version of every document matching `filter`, then deletes all of
    /// them.
    DeleteMany {
        /// The filter selecting which documents to delete.
        filter: Document,
    },
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
        Self::new_impl(db, collection_name, history_name, true)
    }

    /// Like [`new`](Self::new), but never uses transactions — see the "Non-transactional
    /// mode" section on this type's docs for the consistency tradeoff this implies.
    pub fn new_standalone(db: &Database, collection_name: &str) -> Self {
        let history_name = format!("{collection_name}_history");
        Self::with_history_name_standalone(db, collection_name, &history_name)
    }

    /// Like [`with_history_name`](Self::with_history_name), but never uses transactions — see
    /// the "Non-transactional mode" section on this type's docs for the consistency tradeoff
    /// this implies.
    pub fn with_history_name_standalone(
        db: &Database,
        collection_name: &str,
        history_name: &str,
    ) -> Self {
        Self::new_impl(db, collection_name, history_name, false)
    }

    fn new_impl(
        db: &Database,
        collection_name: &str,
        history_name: &str,
        transactional: bool,
    ) -> Self {
        Self {
            client: db.client().clone(),
            collection: db.collection(collection_name),
            history: db.collection(history_name),
            transactional,
        }
    }

    /// The wrapped main collection, for reads and any operation not covered by this type
    /// (e.g. `find`, `find_one`, aggregation).
    pub fn collection(&self) -> &Collection<T> {
        &self.collection
    }

    /// The history collection that archived versions are stored in.
    pub fn history(&self) -> &Collection<HistoryEntry<T>> {
        &self.history
    }

    /// Creates a TTL index on the history collection's `archived_at` field, so MongoDB
    /// automatically deletes archived entries older than `max_age`.
    ///
    /// This is best-effort and server-driven: MongoDB sweeps for expired documents roughly
    /// once every 60 seconds, so entries may briefly outlive `max_age` before being removed.
    /// For deterministic on-demand deletion, use [`prune_history_older_than`] instead.
    ///
    /// Safe to call more than once (e.g. on every application startup) with the same
    /// `max_age` — creating an identical index is a no-op. Calling it again with a
    /// *different* `max_age` requires the existing index to be dropped first (MongoDB rejects
    /// changing an existing TTL index's expiry via `create_index`); the driver's error in that
    /// case is returned as-is so the caller can decide whether to drop and recreate.
    ///
    /// [`prune_history_older_than`]: Self::prune_history_older_than
    pub async fn ensure_history_ttl_index(&self, max_age: Duration) -> Result<()> {
        let model = IndexModel::builder()
            .keys(doc! { "archived_at": 1 })
            .options(IndexOptions::builder().expire_after(max_age).build())
            .build();
        self.history.create_index(model).await?;
        Ok(())
    }

    /// Deletes every history entry archived more than `max_age` ago. Unlike the TTL index,
    /// this runs immediately and deterministically when called, rather than waiting on
    /// MongoDB's background TTL sweep. Returns the number of deleted entries.
    pub async fn prune_history_older_than(&self, max_age: Duration) -> Result<u64> {
        let cutoff = bson::DateTime::from_system_time(SystemTime::now() - max_age);
        let result = self
            .history
            .delete_many(doc! { "archived_at": { "$lt": cutoff } })
            .await?;
        Ok(result.deleted_count)
    }

    /// Inserts `document`. Nothing is archived, since there is no previous version of a new
    /// document. A thin passthrough to the wrapped collection's `insert_one`, provided so
    /// callers don't need to reach through `.collection()` for the one CRUD operation that has
    /// nothing to archive.
    pub async fn insert_one(&self, document: impl Borrow<T> + Send) -> Result<InsertOneResult> {
        Ok(self.collection.insert_one(document).await?)
    }

    /// Inserts `documents`. Nothing is archived, since there is no previous version of a new
    /// document. A thin passthrough to the wrapped collection's `insert_many`.
    pub async fn insert_many(
        &self,
        documents: impl IntoIterator<Item = impl Borrow<T>> + Send,
    ) -> Result<InsertManyResult> {
        Ok(self.collection.insert_many(documents).await?)
    }

    /// Archives the current version of every document matching `filter`, then applies
    /// `update` to it. Atomic unless this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone) — see the
    /// "Non-transactional mode" section on this type's docs.
    pub async fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        if self.transactional {
            self.run_transaction(filter, Operation::Update, async |session, filter| {
                self.collection
                    .update_one(filter, update.clone())
                    .session(session)
                    .await
            })
            .await
        } else {
            self.archive_matching_standalone(filter.clone(), Operation::Update)
                .await?;
            Ok(self.collection.update_one(filter, update).await?)
        }
    }

    /// Archives the current version of every document matching `filter`, then applies
    /// `update` to all of them. Atomic unless this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone) — see the
    /// "Non-transactional mode" section on this type's docs.
    pub async fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        if self.transactional {
            self.run_transaction(filter, Operation::Update, async |session, filter| {
                self.collection
                    .update_many(filter, update.clone())
                    .session(session)
                    .await
            })
            .await
        } else {
            self.archive_matching_standalone(filter.clone(), Operation::Update)
                .await?;
            Ok(self.collection.update_many(filter, update).await?)
        }
    }

    /// Archives the current version of the document matching `filter`, then replaces it
    /// with `replacement`. Atomic unless this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone) — see the
    /// "Non-transactional mode" section on this type's docs.
    pub async fn replace_one(&self, filter: Document, replacement: T) -> Result<UpdateResult> {
        if self.transactional {
            self.run_transaction(filter, Operation::Replace, async |session, filter| {
                self.collection
                    .replace_one(filter, replacement.clone())
                    .session(session)
                    .await
            })
            .await
        } else {
            self.archive_matching_standalone(filter.clone(), Operation::Replace)
                .await?;
            Ok(self.collection.replace_one(filter, replacement).await?)
        }
    }

    /// Archives the current version of the document matching `filter`, then deletes it.
    /// Atomic unless this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone) — see the
    /// "Non-transactional mode" section on this type's docs.
    pub async fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        if self.transactional {
            self.run_transaction(filter, Operation::Delete, async |session, filter| {
                self.collection.delete_one(filter).session(session).await
            })
            .await
        } else {
            self.archive_matching_standalone(filter.clone(), Operation::Delete)
                .await?;
            Ok(self.collection.delete_one(filter).await?)
        }
    }

    /// Archives the current version of every document matching `filter`, then deletes all
    /// of them. Atomic unless this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone) — see the
    /// "Non-transactional mode" section on this type's docs.
    pub async fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        if self.transactional {
            self.run_transaction(filter, Operation::Delete, async |session, filter| {
                self.collection.delete_many(filter).session(session).await
            })
            .await
        } else {
            self.archive_matching_standalone(filter.clone(), Operation::Delete)
                .await?;
            Ok(self.collection.delete_many(filter).await?)
        }
    }

    /// Submits every model in `models` as a single MongoDB `bulkWrite` command, archiving the
    /// current version of every document any update/replace/delete model matches before the
    /// batch is applied — all atomically, within one transaction.
    ///
    /// # Requirements
    ///
    /// In addition to the replica-set requirement common to every mutating method on this
    /// type, `bulk_write` requires **MongoDB server 8.0 or later** (the `bulkWrite` command
    /// this method uses does not exist on older servers).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use mongodb::Client;
    /// # use mongokeeper::{BulkWriteModel, TrackedCollection};
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct Order { #[serde(rename = "_id")] id: bson::oid::ObjectId, status: String }
    /// # async fn run() -> mongokeeper::Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let db = client.database("shop");
    /// let orders: TrackedCollection<Order> = TrackedCollection::new(&db, "orders");
    /// orders
    ///     .bulk_write(vec![
    ///         BulkWriteModel::UpdateMany {
    ///             filter: bson::doc! { "status": "pending" },
    ///             update: bson::doc! { "$set": { "status": "shipped" } },
    ///         },
    ///         BulkWriteModel::DeleteOne {
    ///             filter: bson::doc! { "status": "cancelled" },
    ///         },
    ///     ])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bulk_write(
        &self,
        models: Vec<BulkWriteModel<T>>,
    ) -> Result<SummaryBulkWriteResult> {
        if self.transactional {
            self.with_transaction(async |session| {
                let mut write_models = Vec::with_capacity(models.len());
                for model in &models {
                    if let Some((filter, operation)) = model.archive_target() {
                        self.archive_matching(session, filter, operation).await?;
                    }
                    write_models.push(model.to_write_model(&self.collection)?);
                }

                self.client.bulk_write(write_models).session(session).await
            })
            .await
        } else {
            let mut write_models = Vec::with_capacity(models.len());
            for model in &models {
                if let Some((filter, operation)) = model.archive_target() {
                    self.archive_matching_standalone(filter, operation).await?;
                }
                write_models.push(model.to_write_model(&self.collection)?);
            }

            Ok(self.client.bulk_write(write_models).await?)
        }
    }

    /// Runs the archive-then-mutate pattern shared by every single-operation mutating method
    /// inside a transaction: read every document matching `filter`, insert a [`HistoryEntry`]
    /// for each into the history collection, then run `mutate`, then commit.
    async fn run_transaction<R>(
        &self,
        filter: Document,
        operation: Operation,
        mut mutate: impl AsyncFnMut(&mut ClientSession, Document) -> mongodb::error::Result<R>,
    ) -> Result<R> {
        self.with_transaction(async |session| {
            self.archive_matching(session, filter.clone(), operation)
                .await?;
            mutate(session, filter.clone()).await
        })
        .await
    }

    /// Runs `body` inside a MongoDB transaction and commits it.
    ///
    /// If `body` fails with a transient error (a write conflict, a replica set election, etc.),
    /// the whole attempt is retried from scratch, since the transaction's read snapshot no
    /// longer applies. Retries continue for up to [`RETRY_TIMEOUT`] before the error is
    /// returned to the caller.
    async fn with_transaction<R>(
        &self,
        mut body: impl AsyncFnMut(&mut ClientSession) -> mongodb::error::Result<R>,
    ) -> Result<R> {
        let deadline = Instant::now() + RETRY_TIMEOUT;

        loop {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;

            match body(&mut session).await {
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

    /// Like [`archive_matching`](Self::archive_matching), but without a session — the
    /// non-transactional counterpart used when this instance was constructed with
    /// [`new_standalone`](Self::new_standalone)/
    /// [`with_history_name_standalone`](Self::with_history_name_standalone).
    async fn archive_matching_standalone(
        &self,
        filter: Document,
        operation: Operation,
    ) -> mongodb::error::Result<()> {
        let mut cursor = self.collection.find(filter).await?;
        let mut entries = Vec::new();
        while let Some(document) = cursor.try_next().await? {
            entries.push(HistoryEntry {
                archived_at: bson::DateTime::now(),
                operation,
                document,
            });
        }

        if !entries.is_empty() {
            self.history.insert_many(entries).await?;
        }

        Ok(())
    }
}

impl<T> BulkWriteModel<T>
where
    T: Serialize + Send + Sync,
{
    /// The filter and [`Operation`] to archive under before this model is applied, or `None`
    /// for `InsertOne` (nothing to archive).
    fn archive_target(&self) -> Option<(Document, Operation)> {
        match self {
            BulkWriteModel::InsertOne(_) => None,
            BulkWriteModel::UpdateOne { filter, .. }
            | BulkWriteModel::UpdateMany { filter, .. } => {
                Some((filter.clone(), Operation::Update))
            }
            BulkWriteModel::ReplaceOne { filter, .. } => Some((filter.clone(), Operation::Replace)),
            BulkWriteModel::DeleteOne { filter } | BulkWriteModel::DeleteMany { filter } => {
                Some((filter.clone(), Operation::Delete))
            }
        }
    }

    /// Converts this model into the driver's [`WriteModel`], scoped to `collection`'s
    /// namespace.
    fn to_write_model(&self, collection: &Collection<T>) -> mongodb::error::Result<WriteModel> {
        let namespace = collection.namespace();
        Ok(match self {
            BulkWriteModel::InsertOne(document) => collection.insert_one_model(document)?.into(),
            BulkWriteModel::UpdateOne { filter, update } => UpdateOneModel::builder()
                .namespace(namespace)
                .filter(filter.clone())
                .update(UpdateModifications::Document(update.clone()))
                .build()
                .into(),
            BulkWriteModel::UpdateMany { filter, update } => UpdateManyModel::builder()
                .namespace(namespace)
                .filter(filter.clone())
                .update(UpdateModifications::Document(update.clone()))
                .build()
                .into(),
            BulkWriteModel::ReplaceOne {
                filter,
                replacement,
            } => collection
                .replace_one_model(filter.clone(), replacement)?
                .into(),
            BulkWriteModel::DeleteOne { filter } => DeleteOneModel::builder()
                .namespace(namespace)
                .filter(filter.clone())
                .build()
                .into(),
            BulkWriteModel::DeleteMany { filter } => DeleteManyModel::builder()
                .namespace(namespace)
                .filter(filter.clone())
                .build()
                .into(),
        })
    }
}

fn is_transient_transaction_error(err: &mongodb::error::Error) -> bool {
    err.contains_label(TRANSIENT_TRANSACTION_ERROR)
}

fn is_unknown_commit_result(err: &mongodb::error::Error) -> bool {
    err.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
}

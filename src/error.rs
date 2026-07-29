//! Error type for MongoKeeper operations.

/// Errors that can occur while using a [`crate::TrackedCollection`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error returned by the underlying MongoDB driver.
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

//! Types describing archived historical document versions.

use serde::{Deserialize, Serialize};

/// The kind of mutation that caused a document's previous version to be archived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// The document was updated (partial modification via an update document/pipeline).
    Update,
    /// The document was replaced in full.
    Replace,
    /// The document was deleted.
    Delete,
}

/// A single archived version of a document, stored in a collection's history collection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryEntry<T> {
    /// The time at which this version was archived, i.e. immediately before the mutation
    /// that superseded it was applied.
    pub archived_at: bson::DateTime,
    /// The operation that caused this version to be archived.
    pub operation: Operation,
    /// The full document as it existed immediately before the mutation was applied.
    pub document: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        count: i32,
    }

    #[test]
    fn operation_round_trips_through_bson() {
        for op in [Operation::Update, Operation::Replace, Operation::Delete] {
            let value = bson::serialize_to_bson(&op).expect("serialize operation");
            let back: Operation =
                bson::deserialize_from_bson(value).expect("deserialize operation");
            assert_eq!(op, back);
        }
    }

    #[test]
    fn history_entry_round_trips_through_bson() {
        let entry = HistoryEntry {
            archived_at: bson::DateTime::now(),
            operation: Operation::Update,
            document: Sample {
                name: "widget".to_string(),
                count: 3,
            },
        };

        let bytes = bson::serialize_to_vec(&entry).expect("serialize history entry");
        let back: HistoryEntry<Sample> =
            bson::deserialize_from_slice(&bytes).expect("deserialize history entry");
        assert_eq!(entry, back);
    }
}

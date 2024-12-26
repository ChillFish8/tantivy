//! Internal operations for working with single segments
//!
//! WARNING: This is an advanced module, and you shouldn't use the things in here
//! unless you absolutely know what you're doing.

use crate::query::Weight;
use crate::schema::document::Document;
use crate::schema::{TantivyDocument, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation {
    /// The [Opstamp] assigned to this operation.
    pub opstamp: Opstamp,
    /// The weight used for selecting the documents to delete.
    pub target: Box<dyn Weight>,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation<D: Document = TantivyDocument> {
    /// The [Opstamp] assigned to this operation.
    pub opstamp: Opstamp,
    /// The document data to be indexed.
    pub document: D,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation<D: Document = TantivyDocument> {
    /// Add operation
    Add(D),
    /// Delete operation
    Delete(Term),
}

use crate::query::Weight;
use crate::schema::{Document, DocumentAccess, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation<D: DocumentAccess = Document> {
    pub opstamp: Opstamp,
    pub target: Box<dyn Weight<D>>,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation<D = Document>
where
    D: DocumentAccess
{
    pub opstamp: Opstamp,
    pub document: D,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation<D: DocumentAccess = Document> {
    /// Add operation
    Add(D),
    /// Delete operation
    Delete(Term),
}

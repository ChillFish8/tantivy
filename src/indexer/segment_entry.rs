use std::fmt;

use common::BitSet;

use crate::core::{SegmentId, SegmentMeta};
use crate::Document;
use crate::indexer::delete_queue::DeleteCursor;
use crate::schema::DocumentAccess;

/// A segment entry describes the state of
/// a given segment, at a given instant.
///
/// In addition to segment `meta`,
/// it contains a few transient states
/// - `alive_bitset` is a bitset describing
/// documents that were alive during the commit
/// itself.
/// - `delete_cursor` is the position in the delete queue.
/// Deletes happening before the cursor are reflected either
/// in the .del file or in the `alive_bitset`.
pub struct SegmentEntry<D: DocumentAccess = Document> {
    meta: SegmentMeta,
    alive_bitset: Option<BitSet>,
    delete_cursor: DeleteCursor<D>,
}

impl<D: DocumentAccess> Clone for SegmentEntry<D> {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            alive_bitset: self.alive_bitset.clone(),
            delete_cursor: self.delete_cursor.clone(),
        }
    }
}

impl<D: DocumentAccess> SegmentEntry<D> {
    /// Create a new `SegmentEntry`
    pub fn new(
        segment_meta: SegmentMeta,
        delete_cursor: DeleteCursor<D>,
        alive_bitset: Option<BitSet>,
    ) -> Self {
        Self {
            meta: segment_meta,
            alive_bitset,
            delete_cursor,
        }
    }

    /// Return a reference to the segment entry deleted bitset.
    ///
    /// `DocId` in this bitset are flagged as deleted.
    pub fn alive_bitset(&self) -> Option<&BitSet> {
        self.alive_bitset.as_ref()
    }

    /// Set the `SegmentMeta` for this segment.
    pub fn set_meta(&mut self, segment_meta: SegmentMeta) {
        self.meta = segment_meta;
    }

    /// Return a reference to the segment_entry's delete cursor
    pub fn delete_cursor(&mut self) -> &mut DeleteCursor<D> {
        &mut self.delete_cursor
    }

    /// Returns the segment id.
    pub fn segment_id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Accessor to the `SegmentMeta`
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }
}

impl fmt::Debug for SegmentEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SegmentEntry({:?})", self.meta)
    }
}

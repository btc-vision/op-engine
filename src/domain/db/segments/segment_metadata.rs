use crate::domain::db::segments::b_tree_index::BTreeIndex;
use std::path::PathBuf;
use std::sync::Arc;

/// On-disk segment files store key-value pairs plus metadata
/// about which block range they cover. During reorg, we can discard
/// segments covering blocks above the reorg height.
#[derive(Debug)]
pub struct SegmentMetadata {
    pub file_path: PathBuf,
    pub start_height: u64,
    pub end_height: u64,

    // We store an Arc to the BTreeIndex in memory
    pub index: Option<Arc<BTreeIndex>>,
}

impl SegmentMetadata {
    pub fn is_above_height(&self, target_height: u64) -> bool {
        self.start_height > target_height
    }
}

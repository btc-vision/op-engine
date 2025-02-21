use std::collections::VecDeque;
use std::io::{Read, Write};

use crate::domain::generic::errors::{OpNetError, OpNetResult};

/// Max keys per node (for simplicity).
const MAX_KEYS: usize = 256;

/// A helper struct for BFS-based deserialization.
struct NodeMeta {
    is_leaf: bool,
    keys: Vec<Vec<u8>>,
    values: Vec<u64>,
    child_count: usize,
}

#[derive(Debug)]
struct BTreeNode {
    keys: Vec<Vec<u8>>,
    values: Vec<u64>,
    children: Vec<Box<BTreeNode>>,
    is_leaf: bool,
}

impl BTreeNode {
    fn new(is_leaf: bool) -> Self {
        BTreeNode {
            keys: Vec::with_capacity(MAX_KEYS),
            values: Vec::with_capacity(MAX_KEYS),
            children: Vec::with_capacity(MAX_KEYS + 1),
            is_leaf,
        }
    }
}

impl Default for BTreeNode {
    fn default() -> Self {
        BTreeNode::new(true)
    }
}

#[derive(Debug)]
pub struct BTreeIndex {
    root: BTreeNode,
}

impl BTreeIndex {
    pub fn new() -> Self {
        BTreeIndex {
            root: BTreeNode::new(true),
        }
    }

    /// Insert a (key, offset) pair into the B-Tree.
    pub fn insert(&mut self, key: Vec<u8>, offset: u64) {
        if self.root.keys.len() == MAX_KEYS {
            // Root is full; split first.
            let mut new_root = BTreeNode::new(false);
            new_root.children.push(Box::new(std::mem::replace(
                &mut self.root,
                BTreeNode::new(true),
            )));
            split_child(&mut new_root, 0);
            insert_non_full(&mut new_root, key, offset);
            self.root = new_root;
        } else {
            insert_non_full(&mut self.root, key, offset);
        }
    }

    /// Look up a single key in the B-Tree.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        search_node(&self.root, key)
    }

    /// Return all `(key, offset)` pairs in the inclusive range `[start_key..end_key]`,
    /// up to `limit`. An in-order traversal is used to maintain ascending order of keys.
    ///
    /// If you only need the offsets, you can drop the `key` in the result tuple.
    pub fn range_search(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: Option<usize>,
    ) -> Vec<(Vec<u8>, u64)> {
        let mut results = Vec::new();
        range_search_node(&self.root, start_key, end_key, limit, &mut results);
        results
    }

    /// Write the entire B-Tree to `w` in BFS order.
    pub fn write_to_disk<W: Write>(&self, w: &mut W) -> OpNetResult<()> {
        let mut queue = VecDeque::new();
        queue.push_back(&self.root);

        let mut bfs_nodes = Vec::new();
        while let Some(node) = queue.pop_front() {
            bfs_nodes.push(node);
            if !node.is_leaf {
                for child in &node.children {
                    queue.push_back(child);
                }
            }
        }

        // 1) Write out the total number of nodes
        let node_count = bfs_nodes.len() as u64;
        w.write_all(&node_count.to_le_bytes())
            .map_err(|e| OpNetError::new(&format!("B-Tree write node_count: {}", e)))?;

        // 2) Write each node in BFS order
        for node in bfs_nodes {
            // Write one byte for is_leaf
            let leaf_byte = if node.is_leaf { 1 } else { 0 };
            w.write_all(&[leaf_byte])
                .map_err(|e| OpNetError::new(&format!("B-Tree write leaf: {}", e)))?;

            // number of keys (8 bytes)
            let kcount = node.keys.len() as u64;
            w.write_all(&kcount.to_le_bytes())
                .map_err(|e| OpNetError::new(&format!("B-Tree write kcount: {}", e)))?;

            // each (key, value)
            for (k, &val) in node.keys.iter().zip(node.values.iter()) {
                let klen = k.len() as u64;
                w.write_all(&klen.to_le_bytes())
                    .map_err(|e| OpNetError::new(&format!("B-Tree write klen: {}", e)))?;
                w.write_all(k)
                    .map_err(|e| OpNetError::new(&format!("B-Tree write key: {}", e)))?;
                w.write_all(&val.to_le_bytes())
                    .map_err(|e| OpNetError::new(&format!("B-Tree write offset: {}", e)))?;
            }

            // number of children (8 bytes)
            let ccount = node.children.len() as u64;
            w.write_all(&ccount.to_le_bytes())
                .map_err(|e| OpNetError::new(&format!("B-Tree write ccount: {}", e)))?;
        }
        Ok(())
    }

    /// Read a B-Tree from `r` that was written in BFS order by `write_to_disk`.
    pub fn read_from_disk<R: Read>(r: &mut R) -> OpNetResult<Self> {
        // 1) Read node_count (8 bytes)
        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)
            .map_err(|e| OpNetError::new(&format!("B-Tree read node_count: {}", e)))?;
        let node_count = u64::from_le_bytes(buf8) as usize;

        if node_count == 0 {
            // No nodes => empty tree
            return Ok(BTreeIndex::new());
        }

        // 2) Read BFS node metadata
        let mut meta_vec = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            // is_leaf: read 1 byte
            let mut leaf_buf = [0u8; 1];
            r.read_exact(&mut leaf_buf)
                .map_err(|e| OpNetError::new(&format!("B-Tree read is_leaf: {}", e)))?;
            let is_leaf = (leaf_buf[0] == 1);

            // key count (8 bytes)
            let mut kcount_buf = [0u8; 8];
            r.read_exact(&mut kcount_buf)
                .map_err(|e| OpNetError::new(&format!("B-Tree read kcount: {}", e)))?;
            let kcount = u64::from_le_bytes(kcount_buf) as usize;

            let mut keys = Vec::with_capacity(kcount);
            let mut values = Vec::with_capacity(kcount);

            for _ in 0..kcount {
                // read key length (8 bytes)
                let mut klen_buf = [0u8; 8];
                r.read_exact(&mut klen_buf)
                    .map_err(|e| OpNetError::new(&format!("B-Tree read klen: {}", e)))?;
                let klen = u64::from_le_bytes(klen_buf) as usize;

                // read key bytes
                let mut key_vec = vec![0u8; klen];
                r.read_exact(&mut key_vec)
                    .map_err(|e| OpNetError::new(&format!("B-Tree read key: {}", e)))?;

                // read offset
                let mut off_buf = [0u8; 8];
                r.read_exact(&mut off_buf)
                    .map_err(|e| OpNetError::new(&format!("B-Tree read offset: {}", e)))?;
                let offset = u64::from_le_bytes(off_buf);

                keys.push(key_vec);
                values.push(offset);
            }

            // read number of children (8 bytes)
            let mut ccount_buf = [0u8; 8];
            r.read_exact(&mut ccount_buf)
                .map_err(|e| OpNetError::new(&format!("B-Tree read ccount: {}", e)))?;
            let child_count = u64::from_le_bytes(ccount_buf) as usize;

            meta_vec.push(NodeMeta {
                is_leaf,
                keys,
                values,
                child_count,
            });
        }

        // 3) Create node list in BFS order
        let mut nodes: Vec<Box<BTreeNode>> = Vec::with_capacity(node_count);
        for meta in &meta_vec {
            nodes.push(Box::new(BTreeNode {
                keys: meta.keys.clone(),
                values: meta.values.clone(),
                children: Vec::with_capacity(meta.child_count),
                is_leaf: meta.is_leaf,
            }));
        }

        // 4) Link children
        let mut next_child_index = 1;
        for (i, meta) in meta_vec.iter().enumerate() {
            let child_count = meta.child_count;
            if child_count == 0 {
                continue;
            }

            let end = next_child_index + child_count;
            if end > node_count {
                return Err(OpNetError::new(&format!(
                    "B-Tree read: invalid child_count at node {} (exceeds total).",
                    i
                )));
            }

            for _ in next_child_index..end {
                let child_node =
                    std::mem::replace(&mut nodes[next_child_index], Box::new(BTreeNode::new(true)));
                nodes[i].children.push(child_node);
                next_child_index += 1;
            }
        }

        // The root is the first node
        let root = *nodes.remove(0);
        Ok(BTreeIndex { root })
    }
}

// --------------------------------------------------------
//  Internal Helper Routines
// --------------------------------------------------------

/// Insert (key, offset) into a node known not to be full.
fn insert_non_full(node: &mut BTreeNode, key: Vec<u8>, offset: u64) {
    if node.is_leaf {
        // Insert into the keys array in sorted order
        let pos = match node.keys.binary_search(&key) {
            Ok(pos) => {
                // Key already exists => update
                node.values[pos] = offset;
                return;
            }
            Err(pos) => pos,
        };
        node.keys.insert(pos, key);
        node.values.insert(pos, offset);
    } else {
        // Find child
        let mut i = match node.keys.binary_search(&key) {
            Ok(pos) => {
                node.values[pos] = offset;
                return;
            }
            Err(pos) => pos,
        };
        // If that child is full, split
        if node.children[i].keys.len() == MAX_KEYS {
            split_child(node, i);
            if key > node.keys[i] {
                i += 1;
            }
        }
        insert_non_full(&mut node.children[i], key, offset);
    }
}

/// Split child `child_index` of `parent` around the median key, promoting that
/// median up into the parent.
fn split_child(parent: &mut BTreeNode, child_index: usize) {
    let mid = MAX_KEYS / 2;
    let child = &mut parent.children[child_index];

    let mut new_node = BTreeNode::new(child.is_leaf);

    // Move [mid+1..] keys/values to new_node
    new_node.keys.extend_from_slice(&child.keys[mid + 1..]);
    new_node.values.extend_from_slice(&child.values[mid + 1..]);

    if !child.is_leaf {
        new_node
            .children
            .extend(child.children.drain(mid + 1..).collect::<Vec<_>>());
    }

    let up_key = child.keys[mid].clone();
    let up_val = child.values[mid];

    child.keys.truncate(mid);
    child.values.truncate(mid);

    parent.keys.insert(child_index, up_key);
    parent.values.insert(child_index, up_val);

    parent.children.insert(child_index + 1, Box::new(new_node));
}

/// Find a single key recursively.
fn search_node(node: &BTreeNode, key: &[u8]) -> Option<u64> {
    match node
        .keys
        .binary_search_by(|probe| probe.as_slice().cmp(key))
    {
        Ok(pos) => Some(node.values[pos]),
        Err(pos) => {
            if node.is_leaf {
                None
            } else {
                search_node(&node.children[pos], key)
            }
        }
    }
}

/// Recursively search [start_key..end_key] in ascending order.
/// Collect up to `limit` results, storing `(key, offset)` in `results`.
///
/// If limit is None, collects all. If limit is Some(n), we stop after n results.
fn range_search_node(
    node: &BTreeNode,
    start_key: &[u8],
    end_key: &[u8],
    limit: Option<usize>,
    results: &mut Vec<(Vec<u8>, u64)>,
) {
    // We'll do an in-order traversal:
    //   for i in 0..keys.len():
    //     explore children[i] (if not leaf)
    //     check keys[i]
    //   then children[last] if not leaf
    //
    // We skip keys < start_key or keys > end_key, but we must still recurse children if needed.
    let mut i = 0;
    while i < node.keys.len() {
        // Recurse left child first
        if !node.is_leaf {
            range_search_node(&node.children[i], start_key, end_key, limit, results);
            // If we've reached our limit, stop
            if let Some(lim) = limit {
                if results.len() >= lim {
                    return;
                }
            }
        }

        // Now check the node.keys[i]
        let key_slice = &node.keys[i];
        if key_slice.as_slice() >= start_key && key_slice.as_slice() <= end_key {
            results.push((key_slice.clone(), node.values[i]));
            if let Some(lim) = limit {
                if results.len() >= lim {
                    return;
                }
            }
        } else if key_slice.as_slice() > end_key {
            // Because keys are sorted ascending, if we found one that's > end_key,
            // we can stop scanning further keys in this node.
            return;
        }

        i += 1;
    }

    // Finally, recurse the rightmost child
    if !node.is_leaf {
        range_search_node(&node.children[i], start_key, end_key, limit, results);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A helper to create a BTreeIndex with a sequence of key->offset inserts.
    fn build_btree(entries: &[(Vec<u8>, u64)]) -> BTreeIndex {
        let mut btree = BTreeIndex::new();
        for (k, v) in entries {
            btree.insert(k.clone(), *v);
        }
        btree
    }

    /// Verifies that the BTreeIndex returns expected values for each key in `entries`.
    /// Also checks that some unknown key does not exist.
    fn verify_btree_contents(btree: &BTreeIndex, entries: &[(Vec<u8>, u64)]) {
        for (k, v) in entries {
            assert_eq!(
                btree.get(k),
                Some(*v),
                "Expected to find key {:?} => {}",
                k,
                v
            );
        }
        assert_eq!(btree.get(b"NO_SUCH_KEY"), None);
    }

    #[test]
    fn test_btree_insert_search() {
        let mut btree = BTreeIndex::new();
        btree.insert(b"hello".to_vec(), 42);
        btree.insert(b"world".to_vec(), 84);
        btree.insert(b"foo".to_vec(), 100);
        btree.insert(b"bar".to_vec(), 200);
        btree.insert(b"baz".to_vec(), 300);

        assert_eq!(btree.get(b"hello"), Some(42));
        assert_eq!(btree.get(b"world"), Some(84));
        assert_eq!(btree.get(b"foo"), Some(100));
        assert_eq!(btree.get(b"bar"), Some(200));
        assert_eq!(btree.get(b"baz"), Some(300));
        assert_eq!(btree.get(b"nope"), None);
    }

    #[test]
    fn test_btree_write_read_disk() {
        let entries = [
            (b"hello".to_vec(), 42),
            (b"world".to_vec(), 84),
            (b"foo".to_vec(), 100),
            (b"bar".to_vec(), 200),
            (b"baz".to_vec(), 300),
        ];
        let btree = build_btree(&entries);

        // write to in-memory buffer
        let mut buffer = Vec::new();
        btree.write_to_disk(&mut buffer).unwrap();

        // read back
        let btree2 = BTreeIndex::read_from_disk(&mut Cursor::new(buffer)).unwrap();
        verify_btree_contents(&btree2, &entries);
    }

    /// Test inserting the same key multiple times (duplicates).
    /// The B-Tree code updates the value of an existing key rather than storing duplicates.
    #[test]
    fn test_insert_duplicates() {
        let mut btree = BTreeIndex::new();
        btree.insert(b"dup".to_vec(), 1);
        btree.insert(b"dup".to_vec(), 2);
        btree.insert(b"dup".to_vec(), 999);

        // The last insert should overwrite earlier duplicates
        assert_eq!(btree.get(b"dup"), Some(999));

        // Insert another distinct key
        btree.insert(b"unique".to_vec(), 123);
        assert_eq!(btree.get(b"unique"), Some(123));
    }

    /// Test inserting a zero-length key.
    /// This ensures the code properly handles the edge case of an empty key.
    #[test]
    fn test_insert_zero_length_key() {
        let mut btree = BTreeIndex::new();
        btree.insert(vec![], 555);
        assert_eq!(btree.get(&[]), Some(555));
    }

    /// Test inserting a very large key to ensure the code can handle big keys.
    /// (While 256 is not that large, you can adjust as needed.)
    #[test]
    fn test_insert_large_key() {
        let large_key = vec![b'x'; 1024]; // 1 KB key
        let mut btree = BTreeIndex::new();
        btree.insert(large_key.clone(), 9999);

        assert_eq!(btree.get(&large_key), Some(9999));
        assert_eq!(btree.get(b"nonexistent"), None);
    }

    /// Fill the root node up to capacity + 1 to trigger a split at the root.
    /// This directly tests the `split_child` logic for the root.
    #[test]
    fn test_root_split() {
        let mut btree = BTreeIndex::new();
        // Insert enough entries to force at least one split.
        // MAX_KEYS = 256. Insert 257 to force a root split.
        for i in 0..(MAX_KEYS as u64 + 1) {
            let key = format!("key_{}", i).into_bytes();
            btree.insert(key, i);
        }

        // Verify a few random keys
        assert_eq!(btree.get(b"key_0"), Some(0));
        assert_eq!(btree.get(b"key_1"), Some(1));
        assert_eq!(
            btree.get(format!("key_{}", MAX_KEYS).as_bytes()),
            Some(MAX_KEYS as u64)
        );
    }

    /// Insert a much larger number of entries so that we get multiple levels in the tree.
    /// We won't verify *every* key, but enough to confirm structure and no collisions.
    #[test]
    fn test_multi_level_split() {
        let mut btree = BTreeIndex::new();
        // Insert a bunch of entries (e.g. 2*MAX_KEYS or more)
        let total = 2 * MAX_KEYS + 50;
        for i in 0..total {
            let key = format!("k_{:05}", i).into_bytes();
            btree.insert(key, i as u64);
        }

        // Spot check a few
        assert_eq!(btree.get(b"k_00000"), Some(0));
        assert_eq!(
            btree.get(format!("k_{:05}", total - 1).as_bytes()),
            Some((total - 1) as u64)
        );
        // Check a middle value
        let mid = total / 2;
        let mid_key = format!("k_{:05}", mid).into_bytes();
        assert_eq!(btree.get(&mid_key), Some(mid as u64));
    }

    /// Check an empty tree can be written and read back without issues.
    #[test]
    fn test_write_read_empty_tree() {
        let btree = BTreeIndex::new();

        // write to in-memory buffer
        let mut buffer = Vec::new();
        btree.write_to_disk(&mut buffer).unwrap();

        // read back
        let btree2 = BTreeIndex::read_from_disk(&mut Cursor::new(buffer)).unwrap();

        // Both should be empty
        assert_eq!(btree2.get(b"anything"), None);
    }

    /// Tests an intentionally corrupted/partial read to ensure we get an error.
    /// We simulate reading only part of the data (e.g. missing the child metadata).
    #[test]
    fn test_read_from_disk_incomplete_data() {
        let entries = [(b"abc".to_vec(), 123), (b"def".to_vec(), 456)];
        let btree = build_btree(&entries);

        // Write it all out:
        let mut full_buffer = Vec::new();
        btree.write_to_disk(&mut full_buffer).unwrap();

        // Now truncate the buffer to inject corruption/incompleteness
        let truncated_len = full_buffer.len() / 2;
        let partial_buffer = &full_buffer[0..truncated_len];

        // Attempt to read from the truncated buffer
        let result = BTreeIndex::read_from_disk(&mut Cursor::new(partial_buffer));
        assert!(
            result.is_err(),
            "Expected an error when reading incomplete data"
        );
    }

    /// Testing reading from a buffer that claims there is 1 node, but doesn't
    /// provide enough bytes to fully parse that node.
    #[test]
    fn test_read_from_disk_not_enough_bytes_for_single_node() {
        // This buffer is enough to read the node_count=1 but is incomplete
        // for the rest of the node data (e.g. is_leaf, kcount, etc.).
        let mut buffer = Vec::new();

        // Write node_count = 1
        let node_count: u64 = 1;
        buffer.extend_from_slice(&node_count.to_le_bytes());
        // But do *not* write anything else.

        // This should fail while trying to read the single node's data
        let result = BTreeIndex::read_from_disk(&mut Cursor::new(buffer));
        assert!(result.is_err(), "Expected error on incomplete node data");
    }

    /// Just a quick test verifying that an empty node_count leads to an empty tree.
    /// This is a corner case explicitly handled in read_from_disk.
    #[test]
    fn test_read_empty_node_count() {
        // This buffer says there are 0 nodes in the tree
        let mut buffer = Vec::new();
        let node_count: u64 = 0;
        buffer.extend_from_slice(&node_count.to_le_bytes());

        let btree = BTreeIndex::read_from_disk(&mut Cursor::new(buffer)).unwrap();
        assert_eq!(btree.get(b"anything"), None, "Tree should be empty");
    }

    #[test]
    fn test_btree_range_search() {
        let mut btree = BTreeIndex::new();
        // Insert a set of keys in lexicographical order
        let items = [
            (b"apple".to_vec(), 100),
            (b"banana".to_vec(), 200),
            (b"cherry".to_vec(), 300),
            (b"date".to_vec(), 400),
            (b"grape".to_vec(), 500),
            (b"kiwi".to_vec(), 600),
            (b"orange".to_vec(), 700),
            (b"zebra".to_vec(), 800),
        ];

        for (k, v) in &items {
            btree.insert(k.clone(), *v);
        }

        // 1) Range search from "banana" to "grape" (inclusive), no limit
        // Expect: banana(200), cherry(300), date(400), grape(500)
        let results = btree.range_search(b"banana", b"grape", None);
        assert_eq!(
            results,
            vec![
                (b"banana".to_vec(), 200),
                (b"cherry".to_vec(), 300),
                (b"date".to_vec(), 400),
                (b"grape".to_vec(), 500),
            ],
            "range_search(banana..grape) should return exactly those 4 keys in ascending order"
        );

        // 2) Same range but limit = Some(2)
        // Expect only the first 2 hits in ascending order: banana(200), cherry(300)
        let limited = btree.range_search(b"banana", b"grape", Some(2));
        assert_eq!(
            limited,
            vec![(b"banana".to_vec(), 200), (b"cherry".to_vec(), 300),],
            "range_search(banana..grape, limit=2) should return the first 2 keys"
        );

        // 3) Range that covers everything (from "" to "zzzzzz") with no limit
        // Expect all items in ascending order: apple, banana, cherry, date, grape, kiwi, orange, zebra
        let full = btree.range_search(b"", b"zzzzzz", None);
        let expected_all: Vec<(Vec<u8>, u64)> =
            items.iter().map(|(k, v)| (k.clone(), *v)).collect();
        assert_eq!(
            full, expected_all,
            "range_search(empty..zzzzzz) should return all inserted items"
        );

        // 4) Range that has no matches, e.g. from "blueberry" to "blueberry"
        let none = btree.range_search(b"blueberry", b"blueberry", None);
        assert!(
            none.is_empty(),
            "range_search(blueberry..blueberry) should yield no matches"
        );

        // 5) Just sanity check a range from "banana" to "banana"
        // Should return only banana, if it exists.
        let banana_only = btree.range_search(b"banana", b"banana", None);
        assert_eq!(
            banana_only,
            vec![(b"banana".to_vec(), 200)],
            "range_search(banana..banana) should yield exactly the banana item"
        );
    }
}

use crate::domain::db::segments::b_tree_index::BTreeIndex;
use crate::domain::db::segments::segment_metadata::SegmentMetadata;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter};
use crate::domain::thread::concurrency::ThreadPool;

use crate::domain::db::sharded_memtable::ShardedMemTable;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The manager that handles on‐disk segments:
/// - Each `.seg` file contains key‐value pairs
/// - Each `.idx` file stores a `BTreeIndex` of offsets
/// - We discover existing segments at startup
/// - We can flush a `MemTable` or `ShardedMemTable` into a new segment
/// - We can look up keys by scanning from the newest segment to oldest
/// - We can rollback (remove) segments above a certain block height
pub struct SegmentManager {
    /// Directory where `.seg`/`.idx` files live
    data_dir: PathBuf,

    /// Sorted list of known segments, typically ascending by start_height
    pub segments: Vec<SegmentMetadata>,

    /// For parallel I/O or parallel index loading, store a handle to the same thread pool the DB uses
    thread_pool: Arc<Mutex<ThreadPool>>,

    /// A simple lock that ensures only one flush at a time (avoids interleaving writes)
    flush_lock: Mutex<()>,
}

impl SegmentManager {
    /// Create a new `SegmentManager`, discover existing segments, load their indexes in parallel.
    pub fn new<P: AsRef<Path>>(
        data_path: P,
        thread_pool: Arc<Mutex<ThreadPool>>,
    ) -> OpNetResult<Self> {
        let data_dir = data_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| OpNetError::new(&format!("SegmentManager create dir: {}", e)))?;

        let mut manager = SegmentManager {
            data_dir,
            segments: vec![],
            thread_pool,
            flush_lock: Mutex::new(()),
        };

        manager.discover_existing_segments()?;
        Ok(manager)
    }

    /// ------------------------------------------------------------------
    ///  Discover and load existing segments (.seg + .idx)
    /// ------------------------------------------------------------------
    fn discover_existing_segments(&mut self) -> OpNetResult<()> {
        let entries = std::fs::read_dir(&self.data_dir)
            .map_err(|e| OpNetError::new(&format!("discover_segments: {}", e)))?;

        let mut discovered = vec![];
        for entry in entries {
            let entry = entry.map_err(|e| OpNetError::new(&format!("read_dir entry: {}", e)))?;
            let path = entry.path();
            if path.extension().map(|ext| ext == "seg").unwrap_or(false) {
                if let Some(fname) = path.file_name().and_then(|x| x.to_str()) {
                    // Expect something like "collection_10_15.seg" or "collection_10_15_2.seg"
                    let parts: Vec<&str> = fname.split('.').collect();
                    if let Some(name_and_heights) = parts.get(0) {
                        let seg_parts: Vec<&str> = name_and_heights.split('_').collect();
                        // We need at least 3 parts: [collName, start, end] ignoring trailing suffix
                        if seg_parts.len() >= 3 {
                            if let (Ok(start), Ok(end)) =
                                (seg_parts[1].parse::<u64>(), seg_parts[2].parse::<u64>())
                            {
                                discovered.push(SegmentMetadata {
                                    file_path: path.clone(),
                                    start_height: start,
                                    end_height: end,
                                    index: None,
                                });
                            }
                        }
                    }
                }
            }
        }

        // ------------------------------------------------------------------
        //  Load indexes in parallel using the thread pool
        // ------------------------------------------------------------------
        let mut join_handles = vec![];
        {
            let pool = self.thread_pool.lock().map_err(|_| {
                OpNetError::new("Failed to lock thread pool in discover_existing_segments")
            })?;

            for mut seg_meta in discovered {
                let idx_path = seg_meta.file_path.with_extension("idx");
                // Create a task that loads the BTreeIndex (if present) from disk.
                let handle = pool.execute(move || {
                    if let Ok(file) = std::fs::File::open(&idx_path) {
                        let mut br = std::io::BufReader::new(file);
                        if let Ok(btree) = BTreeIndex::read_from_disk(&mut br) {
                            seg_meta.index = Some(Arc::new(btree));
                        } else {
                            seg_meta.index = None;
                        }
                    }
                    seg_meta
                });
                join_handles.push(handle);
            }
        }

        // Collect the results
        for handle in join_handles {
            let seg_meta = handle.join().map_err(|_| {
                OpNetError::new("Thread pool worker panicked while loading segment index.")
            })?;
            self.segments.push(seg_meta);
        }

        // Sort segments by start_height
        self.segments.sort_by_key(|s| s.start_height);

        Ok(())
    }

    /// Similar to `flush_memtable_to_segment` but iterates over each shard in a `ShardedMemTable`.
    /// Acquires the `flush_lock` so multiple flushes won't interfere.
    /// **Note**: This method does NOT clear the sharded memtable; you can do that after success.
    pub fn flush_sharded_memtable_to_segment(
        &mut self,
        collection_name: &str,
        sharded_mem: &ShardedMemTable,
        flush_block_height: u64,
    ) -> OpNetResult<()> {
        let _guard = self
            .flush_lock
            .lock()
            .map_err(|_| OpNetError::new("Failed to lock flush_lock"))?;

        // If total size is zero, nothing to flush
        if sharded_mem.total_size() == 0 {
            return Ok(());
        }

        let start_height = flush_block_height;
        let end_height = flush_block_height;

        // Build a unique base filename
        let base_seg_filename = format!("{}_{}_{}", collection_name, start_height, end_height);
        let mut seg_file_name = format!("{}.seg", base_seg_filename);
        let mut idx_file_name = format!("{}.idx", base_seg_filename);

        let mut seg_path = self.data_dir.join(&seg_file_name);
        let mut idx_path = self.data_dir.join(&idx_file_name);

        // Ensure unique files if they already exist
        let mut attempt = 2;
        while seg_path.exists() || idx_path.exists() {
            seg_file_name = format!("{}_{}", base_seg_filename, attempt);
            idx_file_name = format!("{}_{}.idx", base_seg_filename, attempt);

            seg_file_name.push_str(".seg");
            seg_path = self.data_dir.join(&seg_file_name);
            idx_path = self.data_dir.join(&idx_file_name);

            attempt += 1;
        }

        // Create & write .seg
        let seg_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&seg_path)
            .map_err(|e| {
                OpNetError::new(&format!(
                    "flush_sharded_memtable_to_segment open seg: {}",
                    e
                ))
            })?;

        let mut seg_writer = ByteWriter::new(BufWriter::new(seg_file));
        let mut btree = BTreeIndex::new();

        // ------------------------------------------------------------------
        //  For each shard, lock it, iterate over (k,v) pairs, write them out
        // ------------------------------------------------------------------
        for shard_mutex in &sharded_mem.shards {
            let shard = shard_mutex.lock().unwrap();
            for (k, v) in shard.data.iter() {
                let offset = seg_writer.position();
                seg_writer.write_var_bytes(k)?; // key
                seg_writer.write_var_bytes(v)?; // value
                btree.insert(k.clone(), offset);
            }
        }

        seg_writer.flush()?;

        // Create the .idx file
        let idx_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&idx_path)
            .map_err(|e| {
                OpNetError::new(&format!(
                    "flush_sharded_memtable_to_segment open idx: {}",
                    e
                ))
            })?;

        let mut bw = BufWriter::new(idx_file);
        btree.write_to_disk(&mut bw)?;
        bw.flush()?;

        // Create a new segment metadata entry
        let meta = SegmentMetadata {
            file_path: seg_path,
            start_height,
            end_height,
            index: Some(Arc::new(btree)),
        };
        self.segments.push(meta);
        self.segments.sort_by_key(|s| s.start_height);

        Ok(())
    }

    // ------------------------------------------------------------------
    //  Querying & reading from segments
    // ------------------------------------------------------------------

    /// Search from newest to oldest segment for a key. If found in a B-tree index, read from `.seg`.
    pub fn find_value_for_key(
        &self,
        collection_name: &str,
        key: &[u8],
    ) -> OpNetResult<Option<Vec<u8>>> {
        // Check from newest to oldest
        for seg in self.segments.iter().rev() {
            // Quick check if the segment belongs to this collection by file prefix
            if let Some(fname) = seg.file_path.file_name().and_then(|x| x.to_str()) {
                if !fname.starts_with(collection_name) {
                    continue;
                }
            }
            if let Some(ref btree) = seg.index {
                if let Some(offset) = btree.get(key) {
                    // Attempt reading from .seg
                    let val = self.read_record_from_segment(&seg.file_path, offset, key)?;
                    if val.is_some() {
                        return Ok(val);
                    }
                }
            }
        }
        Ok(None)
    }

    /// Return up to `limit` values for keys in [start_key, end_key] by scanning from newest to oldest,
    /// skipping duplicates.
    pub fn find_values_in_range(
        &self,
        collection_name: &str,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> OpNetResult<Vec<Vec<u8>>> {
        use std::collections::HashSet;
        let mut results = Vec::new();
        let mut seen_keys = HashSet::new();

        // Again, newest to oldest
        for seg in self.segments.iter().rev() {
            if let Some(fname) = seg.file_path.file_name().and_then(|x| x.to_str()) {
                if !fname.starts_with(collection_name) {
                    continue;
                }
            }

            if let Some(ref btree) = seg.index {
                // Range search in the B-tree index
                let items = btree.range_search(start_key, end_key, Some(limit - results.len()));
                for (k, offset) in items {
                    if seen_keys.contains(&k) {
                        continue;
                    }
                    if let Some(val) = self.read_record_from_segment(&seg.file_path, offset, &k)? {
                        results.push(val);
                        seen_keys.insert(k);
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Reads a record (K,V) from a `.seg` file at the given `offset`, verifying that the stored key == `search_key`.
    pub fn read_record_from_segment(
        &self,
        seg_path: &Path,
        offset: u64,
        search_key: &[u8],
    ) -> OpNetResult<Option<Vec<u8>>> {
        let mut file = std::fs::File::open(seg_path)
            .map_err(|e| OpNetError::new(&format!("read_record open: {}", e)))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| OpNetError::new(&format!("seek: {}", e)))?;

        let mut br = ByteReader::new(&mut file);
        let stored_key = match br.read_var_bytes() {
            Ok(k) => k,
            Err(_) => {
                // offset out-of-bounds or corrupted => return None
                return Ok(None);
            }
        };

        if stored_key != search_key {
            // Key mismatch => not our record
            return Ok(None);
        }

        let val = match br.read_var_bytes() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        Ok(Some(val))
    }

    // ------------------------------------------------------------------
    //  Rollback logic
    // ------------------------------------------------------------------
    /// Remove any segments whose `end_height` is above `height`, also deleting their files from disk.
    pub fn rollback_to_height(&mut self, height: u64) -> OpNetResult<()> {
        // Identify which segments to remove
        let to_remove: Vec<usize> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, seg)| seg.end_height > height)
            .map(|(idx, _)| idx)
            .collect();

        // Remove them in reverse order so we don't mess up indexes
        for idx in to_remove.into_iter().rev() {
            let seg = self.segments.remove(idx);
            // Attempt to delete seg file and idx file
            let _ = std::fs::remove_file(&seg.file_path);
            let idx_path = seg.file_path.with_extension("idx");
            let _ = std::fs::remove_file(idx_path);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::thread::concurrency::ThreadPool;
    use std::fs::{create_dir_all, File};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// Helper to create a default in-memory ShardedMemTable with some dummy data.
    fn make_sharded_memtable(pairs: &[(Vec<u8>, Vec<u8>)]) -> ShardedMemTable {
        // You can tweak shard_count or max_size to fit your needs.
        let shard_count = 4;
        let max_size = 1_000_000;
        let sharded = ShardedMemTable::new(shard_count, max_size);

        for (k, v) in pairs {
            sharded.insert(k.clone(), v.clone());
        }
        sharded
    }

    /// Create a new SegmentManager in a temporary directory.
    fn make_segment_manager() -> (TempDir, SegmentManager) {
        // Create a temp directory so each test is isolated on disk
        let tmp_dir = TempDir::new().expect("Failed to create temp dir");
        let tp = Arc::new(Mutex::new(ThreadPool::new(4)));
        let seg_mgr =
            SegmentManager::new(tmp_dir.path(), tp).expect("Failed to create SegmentManager");
        (tmp_dir, seg_mgr)
    }

    #[test]
    fn test_new_and_empty_discovery() {
        // Tests that a brand new data directory with no segments is discovered cleanly
        let (tmp_dir, seg_mgr) = make_segment_manager();
        assert_eq!(
            seg_mgr.segments.len(),
            0,
            "No segments should be discovered"
        );
        // Check that the directory was created
        assert!(tmp_dir.path().exists());
    }

    /// Test flushing a ShardedMemTable with a few keys.
    #[test]
    fn test_flush_sharded_memtable_to_segment() {
        let tmp_dir = TempDir::new().unwrap();
        let tp = Arc::new(Mutex::new(ThreadPool::new(2)));
        let mut seg_mgr =
            SegmentManager::new(tmp_dir.path(), tp).expect("Failed to create SegmentManager");

        let shard_count = 4;
        let max_size = 1_000_000;
        let sharded = ShardedMemTable::new(shard_count, max_size);

        // Insert some keys across shards
        sharded.insert(b"key1".to_vec(), b"value1".to_vec());
        sharded.insert(b"key2".to_vec(), b"value2".to_vec());
        sharded.insert(b"another".to_vec(), b"someval".to_vec());

        // Flush
        seg_mgr
            .flush_sharded_memtable_to_segment("sharded_test", &sharded, 77)
            .expect("Flush sharded memtable must succeed");

        // Now we should have 1 segment in seg_mgr
        assert_eq!(seg_mgr.segments.len(), 1);

        // Confirm we can read them
        let got1 = seg_mgr.find_value_for_key("sharded_test", b"key1").unwrap();
        assert_eq!(got1, Some(b"value1".to_vec()));

        let got2 = seg_mgr.find_value_for_key("sharded_test", b"key2").unwrap();
        assert_eq!(got2, Some(b"value2".to_vec()));

        let got_a = seg_mgr
            .find_value_for_key("sharded_test", b"another")
            .unwrap();
        assert_eq!(got_a, Some(b"someval".to_vec()));
    }

    #[test]
    fn test_discover_existing_segments() {
        // Manually create files that look like segments + indexes
        let tmp_dir = TempDir::new().unwrap();
        create_dir_all(tmp_dir.path()).unwrap();
        // We'll create something like: "mycoll_10_15.seg" and "mycoll_10_15.idx"
        let seg_file = tmp_dir.path().join("mycoll_10_15.seg");
        let idx_file = tmp_dir.path().join("mycoll_10_15.idx");

        File::create(&seg_file).unwrap(); // empty .seg
        File::create(&idx_file).unwrap(); // empty .idx

        // Now create SegmentManager, it should discover 1 segment
        let tp = Arc::new(Mutex::new(ThreadPool::new(2)));
        let mut seg_mgr = SegmentManager::new(tmp_dir.path(), tp).unwrap();
        assert_eq!(seg_mgr.segments.len(), 1, "One discovered segment expected");
        let seg_meta = &seg_mgr.segments[0];
        assert_eq!(seg_meta.start_height, 10);
        assert_eq!(seg_meta.end_height, 15);
        // Because .idx is empty, seg_meta.index is None or Some w/ empty data?
        // BTreeIndex::read_from_disk() likely fails or yields an empty tree => seg_meta.index=None
        assert!(
            seg_meta.index.is_none(),
            "Index should be None for an empty .idx"
        );
    }

    #[test]
    fn test_find_value_for_key() {
        // We'll flush a small sharded memtable with data, then do lookups
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();
        let sharded = make_sharded_memtable(&[
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
        ]);

        seg_mgr
            .flush_sharded_memtable_to_segment("fruits", &sharded, 50)
            .unwrap();
        // Now the data is in a single segment with block range [50..50]

        // Test an existing key
        let found = seg_mgr.find_value_for_key("fruits", b"apple").unwrap();
        assert_eq!(found, Some(b"red".to_vec()));

        // Test a missing key
        let not_found = seg_mgr.find_value_for_key("fruits", b"grape").unwrap();
        assert!(not_found.is_none());

        // Test a different prefix (collection name mismatch)
        let mismatch = seg_mgr.find_value_for_key("vehicles", b"apple").unwrap();
        assert!(mismatch.is_none());
    }

    #[test]
    fn test_rollback_to_height() {
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // We'll flush 3 different sharded memtables at block=100, 101, 103
        let shard_a = make_sharded_memtable(&[(b"a1".to_vec(), b"val100".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard_a, 100)
            .unwrap();

        let shard_b = make_sharded_memtable(&[(b"b1".to_vec(), b"val101".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard_b, 101)
            .unwrap();

        let shard_c = make_sharded_memtable(&[(b"c1".to_vec(), b"val103".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard_c, 103)
            .unwrap();

        assert_eq!(seg_mgr.segments.len(), 3);

        // Rollback to height=101 => remove the segment above 101 (which is 103)
        seg_mgr.rollback_to_height(101).unwrap();
        assert_eq!(
            seg_mgr.segments.len(),
            2,
            "Should have removed the segment above block 101"
        );

        // The removed segment was block 103
        assert_eq!(seg_mgr.segments[0].start_height, 100);
        assert_eq!(seg_mgr.segments[0].end_height, 100);
        assert_eq!(seg_mgr.segments[1].start_height, 101);
        assert_eq!(seg_mgr.segments[1].end_height, 101);
    }

    #[test]
    fn test_file_cleanup_on_rollback() {
        let (tmp_dir, mut seg_mgr) = make_segment_manager();

        let shard_1 = make_sharded_memtable(&[(b"one".to_vec(), b"111".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("mycoll", &shard_1, 10)
            .unwrap();

        let shard_2 = make_sharded_memtable(&[(b"two".to_vec(), b"222".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("mycoll", &shard_2, 12)
            .unwrap();

        // We have 2 segments: mycoll_10_10 and mycoll_12_12
        assert_eq!(seg_mgr.segments.len(), 2);

        // The second segment is above block 11 => rollback to height=11 => remove the second
        seg_mgr.rollback_to_height(11).unwrap();
        assert_eq!(seg_mgr.segments.len(), 1);

        // Confirm .seg & .idx of removed segment are also removed from disk
        let seg_file_2 = tmp_dir.path().join("mycoll_12_12.seg");
        let idx_file_2 = tmp_dir.path().join("mycoll_12_12.idx");
        assert!(
            !seg_file_2.exists(),
            "Segment file from block12 must be removed"
        );
        assert!(
            !idx_file_2.exists(),
            "Index file from block12 must be removed"
        );
    }

    #[test]
    fn test_concurrent_discovery_runs_ok() {
        // This test ensures the parallel code in discover_existing_segments doesn't crash
        // with multiple segment files. It doesn't do heavy concurrency checks, but verifies
        // the parallel tasks complete and the segment list is filled.

        let tmp_dir = TempDir::new().unwrap();

        // Make 3 segments
        for (start, end) in [(1u64, 1u64), (5, 7), (10, 12)] {
            let seg_file = tmp_dir.path().join(format!("col_{}_{}.seg", start, end));
            let idx_file = tmp_dir.path().join(format!("col_{}_{}.idx", start, end));
            File::create(seg_file).unwrap();
            File::create(idx_file).unwrap();
        }

        let tp = Arc::new(Mutex::new(ThreadPool::new(3)));
        let seg_mgr = SegmentManager::new(tmp_dir.path(), tp).unwrap();
        // We expect 3 segments discovered. The indexes are empty => no BTree => index: None
        assert_eq!(seg_mgr.segments.len(), 3);
        let sorted = &seg_mgr.segments;
        // Ensure they are sorted by start height
        assert_eq!(sorted[0].start_height, 1);
        assert_eq!(sorted[1].start_height, 5);
        assert_eq!(sorted[2].start_height, 10);
    }

    #[test]
    fn test_read_record_from_segment() {
        let (tmp_dir, mut seg_mgr) = make_segment_manager();

        // Create a sharded memtable with one K=[1,2,3], V=[4,5,6]
        let shard = make_sharded_memtable(&[(vec![1, 2, 3], vec![4, 5, 6])]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard, 50)
            .unwrap();

        assert_eq!(seg_mgr.segments.len(), 1);
        let seg = &seg_mgr.segments[0];
        let offset = seg.index.as_ref().unwrap().get(&[1, 2, 3]).unwrap();

        // Manually call read_record_from_segment
        let res = seg_mgr.read_record_from_segment(&seg.file_path, offset, &[1, 2, 3]);
        let val = res.unwrap().unwrap();
        assert_eq!(
            val,
            vec![4, 5, 6],
            "Should read back correct value from .seg"
        );

        // If we mismatch the key, we get None
        let mismatch = seg_mgr.read_record_from_segment(&seg.file_path, offset, &[9, 9]);
        assert!(mismatch.unwrap().is_none());
    }

    #[test]
    fn test_flush_sharded_memtable_multiple_times() {
        // Verifies that multiple flushes create multiple segments and the manager handles them properly.
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // Flush #1
        let shard_1 = make_sharded_memtable(&[(b"hello".to_vec(), b"world".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("coll", &shard_1, 10)
            .unwrap();
        assert_eq!(seg_mgr.segments.len(), 1);

        // Flush #2
        let shard_2 = make_sharded_memtable(&[(b"foo".to_vec(), b"bar".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("coll", &shard_2, 11)
            .unwrap();
        assert_eq!(seg_mgr.segments.len(), 2);

        // Check segment ordering
        assert_eq!(seg_mgr.segments[0].start_height, 10);
        assert_eq!(seg_mgr.segments[1].start_height, 11);

        // Retrieve from first segment (hello->world) and second (foo->bar)
        let found1 = seg_mgr.find_value_for_key("coll", b"hello").unwrap();
        assert_eq!(found1, Some(b"world".to_vec()));
        let found2 = seg_mgr.find_value_for_key("coll", b"foo").unwrap();
        assert_eq!(found2, Some(b"bar".to_vec()));
    }

    #[test]
    fn test_flush_sharded_memtable_concurrent_calls() {
        // Check that calling flush concurrently from multiple threads does not corrupt data or panic.
        // We'll spawn N threads, each flushes a different set of data. Because of `flush_lock`, they should
        // effectively serialize, but we ensure the final state is correct.

        use std::thread;

        let (_tmp_dir, seg_mgr) = make_segment_manager();
        let seg_mgr_arc = Arc::new(Mutex::new(seg_mgr));

        let mut handles = vec![];
        for i in 0..5 {
            let seg_mgr_clone = seg_mgr_arc.clone();
            handles.push(thread::spawn(move || {
                let shard = make_sharded_memtable(&[(
                    format!("key{i}").into_bytes(),
                    format!("val{i}").into_bytes(),
                )]);
                // Acquire a lock on seg_mgr and flush
                let mut guard = seg_mgr_clone.lock().unwrap();
                guard
                    .flush_sharded_memtable_to_segment("concurrent", &shard, 100 + i as u64)
                    .unwrap();
            }));
        }

        // Join all threads
        for h in handles {
            h.join().unwrap();
        }

        // Now check we have 5 segments in ascending block height order
        let seg_mgr_final = seg_mgr_arc.lock().unwrap();
        assert_eq!(seg_mgr_final.segments.len(), 5, "Should have 5 segments");
        for (i, seg) in seg_mgr_final.segments.iter().enumerate() {
            assert_eq!(seg.start_height, 100 + i as u64);
            // Retrieve the key from each
            let key = format!("key{i}");
            let val = format!("val{i}");
            let found = seg_mgr_final
                .find_value_for_key("concurrent", key.as_bytes())
                .unwrap();
            assert_eq!(found, Some(val.into_bytes()));
        }
    }

    #[test]
    fn test_rollback_to_same_height_no_removal() {
        // Rolling back to exactly the highest segment's end_height should not remove that segment.
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // Create a segment at block=10
        let shard = make_sharded_memtable(&[(b"cat".to_vec(), b"meow".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard, 10)
            .unwrap();
        assert_eq!(seg_mgr.segments.len(), 1);

        // Rolling back to the same height => no removal
        seg_mgr.rollback_to_height(10).unwrap();
        assert_eq!(
            seg_mgr.segments.len(),
            1,
            "Segment at block=10 should remain if rollback=10"
        );
    }

    #[test]
    fn test_discover_existing_segments_malformed_filenames() {
        // Ensure that malformed filenames are skipped or do not panic the system.
        let tmp_dir = TempDir::new().unwrap();
        create_dir_all(tmp_dir.path()).unwrap();

        // Create some random files that do not match the naming pattern
        let bad_seg_1 = tmp_dir.path().join("collectionNoHeights.seg"); // no '_' in name
        let bad_seg_2 = tmp_dir.path().join("coll_abc_def.seg"); // not numeric
        let bad_seg_3 = tmp_dir.path().join("partial_20.seg.bak"); // extra extension
        let _ = File::create(&bad_seg_1).unwrap();
        let _ = File::create(&bad_seg_2).unwrap();
        let _ = File::create(&bad_seg_3).unwrap();

        // Create a valid segment
        let good_seg = tmp_dir.path().join("real_5_6.seg");
        File::create(&good_seg).unwrap();

        // Now discover
        let tp = Arc::new(Mutex::new(ThreadPool::new(2)));
        let seg_mgr = SegmentManager::new(tmp_dir.path(), tp).unwrap();

        // We should see exactly 1 discovered segment (the good one).
        assert_eq!(
            seg_mgr.segments.len(),
            1,
            "Only the valid segment is discovered"
        );
        assert_eq!(seg_mgr.segments[0].start_height, 5);
        assert_eq!(seg_mgr.segments[0].end_height, 6);
    }

    #[test]
    fn test_read_record_from_segment_out_of_bounds_offset() {
        // We test the case where the offset is bigger than the file size, so the read fails.
        // `read_var_bytes()` should error. We'll forcibly insert a segment with an invalid offset
        // to see that the method returns `Ok(None)` or an error. (Your code might differ.)

        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // Create an actual .seg file with a single record
        let shard = make_sharded_memtable(&[(b"abc".to_vec(), b"def".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("test", &shard, 1)
            .unwrap();
        assert_eq!(seg_mgr.segments.len(), 1);

        // Manually modify the BTree to produce an impossible offset
        let seg = &mut seg_mgr.segments[0];
        if let Some(ref mut btree) = Arc::get_mut(&mut seg.index.as_mut().unwrap()) {
            // Insert a key with offset = 9999999 => out of file bounds
            btree.insert(b"bogus".to_vec(), 9_999_999);
        }

        // Try reading that bogus key => we want to see if read_record_from_segment gracefully returns None or an error.
        let maybe_val = seg_mgr.find_value_for_key("test", b"bogus");
        match maybe_val {
            Ok(None) => {
                // This is acceptable if the read fails partially and yields None
            }
            Err(e) => {
                // Also possible if read leads to an I/O error. We won't assert which is correct;
                // either is valid so long as we don't panic.
                println!("Got an error as expected: {}", e);
            }
            Ok(Some(_)) => panic!("Should not successfully read a key at invalid offset!"),
        }
    }

    #[test]
    fn test_performance_flush_and_read() {
        use std::time::Instant;

        // We’ll measure how long it takes to:
        // 1) flush multiple large sharded memtables,
        // 2) read random keys back.

        // Adjust these for your desired test scale
        const NUM_FLUSHES: usize = 5;
        const ITEMS_PER_FLUSH: usize = 100_000;

        // Create fresh SegmentManager
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // 1) Measure the time to flush multiple sharded memtables
        let start_flush = Instant::now();
        for flush_index in 0..NUM_FLUSHES {
            let mut data = Vec::with_capacity(ITEMS_PER_FLUSH);
            for i in 0..ITEMS_PER_FLUSH {
                // Key and value as bytes
                let key = format!("key-{}-{}", flush_index, i).into_bytes();
                let val = format!("val-{}-{}", flush_index, i).into_bytes();
                data.push((key, val));
            }
            let sharded_mem = make_sharded_memtable(&data);
            seg_mgr
                .flush_sharded_memtable_to_segment(
                    "perf_test",
                    &sharded_mem,
                    1000 + flush_index as u64,
                )
                .expect("flush memtable failed");
        }
        let flush_duration = start_flush.elapsed();
        println!(
            "Flushed {} sharded memtables of {} items each in: {:?}",
            NUM_FLUSHES, ITEMS_PER_FLUSH, flush_duration
        );

        // 2) Measure the time to read random keys from the segments
        use rand::{rng, Rng};
        let mut rng = rng();
        let start_read = Instant::now();
        // We'll do some random lookups
        let lookups = 50_000;
        for _ in 0..lookups {
            // pick a flush index and an item index
            let f_idx = rng.random_range(0..NUM_FLUSHES);
            let i_idx = rng.random_range(0..ITEMS_PER_FLUSH);
            let key_bytes = format!("key-{}-{}", f_idx, i_idx).into_bytes();

            // Attempt to find it
            let found = seg_mgr.find_value_for_key("perf_test", &key_bytes).unwrap();
            // We expect it to exist
            assert!(found.is_some(), "Failed to find a key that should exist");
        }
        let read_duration = start_read.elapsed();
        println!(
            "Performed {} random lookups in: {:?}",
            lookups, read_duration
        );
    }

    #[test]
    fn test_discover_valid_index_file() {
        // 1) Create a SegmentManager and flush a ShardedMemTable to produce a .seg and .idx on disk
        let (tmp_dir, mut seg_mgr) = make_segment_manager();

        // Write out a sharded memtable with a single key-value pair
        let sharded_mem =
            make_sharded_memtable(&[(b"discovery_key".to_vec(), b"discovery_val".to_vec())]);
        seg_mgr
            .flush_sharded_memtable_to_segment("disc", &sharded_mem, 42)
            .expect("Flush should succeed");

        // Confirm we have exactly one segment
        assert_eq!(seg_mgr.segments.len(), 1, "One segment after flush");
        // Also confirm the .idx file actually exists
        let seg_file_name = format!("disc_{}_{}.seg", 42, 42);
        let idx_file_name = format!("disc_{}_{}.idx", 42, 42);
        let seg_path = tmp_dir.path().join(seg_file_name);
        let idx_path = tmp_dir.path().join(idx_file_name);
        assert!(seg_path.exists(), "Segment file must exist");
        assert!(idx_path.exists(), "Index file must exist");

        // 2) Create a *new* SegmentManager pointing to the same directory.
        //    This forces discover_existing_segments() to run and load the .idx from disk.
        drop(seg_mgr); // drop the old manager
        let tp = Arc::new(Mutex::new(ThreadPool::new(2)));
        let seg_mgr2 = SegmentManager::new(tmp_dir.path(), tp)
            .expect("Re-initializing SegmentManager should succeed");

        // 3) Now the discovered segment should have a non-empty index
        assert_eq!(seg_mgr2.segments.len(), 1, "Should discover 1 segment");
        let seg_meta = &seg_mgr2.segments[0];
        assert!(
            seg_meta.index.is_some(),
            "The discovered segment's index should be Some(...)"
        );

        // 4) Confirm that we can read the same key via find_value_for_key,
        //    which demonstrates that BTreeIndex::read_from_disk actually returned Ok(btree).
        let found = seg_mgr2
            .find_value_for_key("disc", b"discovery_key")
            .expect("find_value_for_key should not error");
        assert_eq!(
            found,
            Some(b"discovery_val".to_vec()),
            "Should read the correct value from the discovered index"
        );
    }
}

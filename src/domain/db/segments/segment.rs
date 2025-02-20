use crate::domain::db::memtable::MemTable;
use crate::domain::db::segments::b_tree_index::BTreeIndex;
use crate::domain::db::segments::segment_metadata::SegmentMetadata;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter};
use crate::domain::thread::concurrency::ThreadPool;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The manager that handles on-disk segments:
/// - Each .seg file contains key-value pairs
/// - Each .idx file stores a BTreeIndex of offsets
/// - We discover existing segments at startup
/// - We can flush a MemTable into a new segment
/// - We can look up keys by scanning from the newest segment to oldest
/// - We can rollback (remove) segments above a certain block height
pub struct SegmentManager {
    data_dir: PathBuf,
    pub segments: Vec<SegmentMetadata>,

    // For parallel I/O, we store a handle to the same thread pool that the DB uses.
    thread_pool: Arc<Mutex<ThreadPool>>,

    // A simple lock that ensures only one flush at a time.
    flush_lock: Mutex<()>,
}

impl SegmentManager {
    /// Create a new `SegmentManager`, discover existing segments, and load their indexes in parallel.
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

    /// Discover existing .seg files and load their indexes (optionally in parallel).
    fn discover_existing_segments(&mut self) -> OpNetResult<()> {
        let entries = std::fs::read_dir(&self.data_dir)
            .map_err(|e| OpNetError::new(&format!("discover_segments: {}", e)))?;

        // Gather metadata from filenames
        let mut discovered = vec![];
        for entry in entries {
            let entry = entry.map_err(|e| OpNetError::new(&format!("read_dir entry: {}", e)))?;
            let path = entry.path();
            if path.extension().map(|e| e == "seg").unwrap_or(false) {
                if let Some(fname) = path.file_name().and_then(|x| x.to_str()) {
                    let parts: Vec<&str> = fname.split('.').collect();
                    if let Some(name_and_heights) = parts.get(0) {
                        let seg_parts: Vec<&str> = name_and_heights.split('_').collect();
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

        // -------------------------------------------------------------------
        // LOAD INDEXES IN PARALLEL
        // -------------------------------------------------------------------
        let mut join_handles = vec![];
        {
            let pool = self.thread_pool.lock().map_err(|_| {
                OpNetError::new("Failed to lock thread pool in discover_existing_segments")
            })?;

            for mut seg_meta in discovered {
                let idx_path = seg_meta.file_path.with_extension("idx");
                // We'll create a task that loads the BTreeIndex (if present) from disk.
                let handle = pool.execute(move || {
                    if let Ok(file) = std::fs::File::open(&idx_path) {
                        let mut br = std::io::BufReader::new(file);
                        if let Ok(btree) = BTreeIndex::read_from_disk(&mut br) {
                            seg_meta.index = Some(Arc::new(btree));
                        }
                    }
                    seg_meta
                });
                join_handles.push(handle);
            }
        }

        // Collect results from each thread.
        for handle in join_handles {
            let seg_meta = handle.join().map_err(|_| {
                OpNetError::new("Thread pool worker panicked while loading segment index.")
            })?;
            self.segments.push(seg_meta);
        }

        // Sort segments by start_height, just to have them in ascending order
        self.segments.sort_by_key(|s| s.start_height);
        Ok(())
    }

    /// Flush the memtable to a new .seg file and a corresponding .idx file.
    /// This method is concurrency-safe thanks to a simple flush_lock.
    pub fn flush_memtable_to_segment(
        &mut self,
        collection_name: &str,
        memtable: &mut MemTable,
        flush_block_height: u64,
    ) -> OpNetResult<()> {
        // Acquire the lock so only one flush can happen at a time
        let _guard = self
            .flush_lock
            .lock()
            .map_err(|_| OpNetError::new("Failed to lock flush_lock"))?;

        if memtable.is_empty() {
            return Ok(());
        }

        // If your memtable can store multiple block heights, you'd track min/max here.
        let start_height = flush_block_height;
        let end_height = flush_block_height;

        let seg_file_name = format!("{}_{}_{}.seg", collection_name, start_height, end_height);
        let seg_path = self.data_dir.join(seg_file_name);

        let idx_file_name = format!("{}_{}_{}.idx", collection_name, start_height, end_height);
        let idx_path = self.data_dir.join(idx_file_name);

        let seg_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&seg_path)
            .map_err(|e| OpNetError::new(&format!("flush_memtable_to_segment open seg: {}", e)))?;

        // Use a buffered writer, but rely on `stream_position()` to get the correct offset
        // even before flushing.
        let mut seg_writer = ByteWriter::new(BufWriter::new(seg_file));

        let mut btree = BTreeIndex::new();

        // For each key/value in memtable, write them to .seg at the current stream position.
        for (k, v) in memtable.data.iter() {
            // Get the in-memory offset, which accounts for everything written so far
            let offset = seg_writer.position();

            seg_writer.write_var_bytes(k)?;
            seg_writer.write_var_bytes(v)?;

            btree.insert(k.clone(), offset);
        }

        // Force all data to be flushed to disk at the end
        seg_writer.flush()?;

        // Write the B-Tree to the .idx file
        {
            let idx_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&idx_path)
                .map_err(|e| {
                    OpNetError::new(&format!("flush_memtable_to_segment open idx: {}", e))
                })?;

            let mut bw = BufWriter::new(idx_file);
            btree.write_to_disk(&mut bw)?;
            bw.flush()
                .map_err(|e| OpNetError::new(&format!("idx flush: {}", e)))?;
        }

        // Add to our in-memory list of segments
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

    /// Find a raw value for the given key by searching from newest to oldest segment.
    pub fn find_value_for_key(
        &self,
        collection_name: &str,
        key: &[u8],
    ) -> OpNetResult<Option<Vec<u8>>> {
        // Check from the newest segments to the oldest
        for seg in self.segments.iter().rev() {
            // check if belongs to the right collection by prefix
            if let Some(fname) = seg.file_path.file_name().and_then(|x| x.to_str()) {
                if !fname.starts_with(collection_name) {
                    continue;
                }
            }

            // If there's a B-tree index, we look up the offset
            if let Some(ref btree) = seg.index {
                if let Some(offset) = btree.get(key) {
                    // read from .seg
                    let val = self.read_record_from_segment(&seg.file_path, offset, key)?;
                    if val.is_some() {
                        return Ok(val);
                    }
                }
            }
        }
        Ok(None)
    }

    /// Read a record (K, V) from a .seg file at a given offset, verifying the key matches.
    fn read_record_from_segment(
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
        let stored_key = br.read_var_bytes()?;
        if stored_key != search_key {
            // collision or mismatch
            return Ok(None);
        }
        let val = br.read_var_bytes()?;
        Ok(Some(val))
    }

    /// Roll back segments above the given height.
    /// If your segments can span multiple blocks, you often want to remove segments
    /// for which `end_height > target_height`. Using only `start_height` can skip partial coverage.
    pub fn rollback_to_height(&mut self, height: u64) -> OpNetResult<()> {
        // remove segments whose end_height is above this height
        let to_remove: Vec<usize> = self
            .segments
            .iter()
            .enumerate()
            .filter(|(_, seg)| seg.end_height > height)
            .map(|(idx, _)| idx)
            .collect();

        for idx in to_remove.into_iter().rev() {
            let seg = self.segments.remove(idx);
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
    use crate::domain::db::memtable::MemTable;
    use crate::domain::thread::concurrency::ThreadPool;
    use std::fs::{create_dir_all, File};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// Helper to create a default in‐memory MemTable with some dummy data.
    fn make_memtable(pairs: &[(Vec<u8>, Vec<u8>)]) -> MemTable {
        let mut memtable = MemTable::new(1_000_000);
        for (k, v) in pairs {
            memtable.data.insert(k.clone(), v.clone());
        }
        memtable
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
    fn test_flush_memtable_to_segment() {
        let (tmp_dir, mut seg_mgr) = make_segment_manager();

        // Start with an empty memtable
        let mut empty_mem = make_memtable(&[]);
        // Flushing empty memtable => no new segments
        seg_mgr
            .flush_memtable_to_segment("coll", &mut empty_mem, 100)
            .unwrap();
        assert!(seg_mgr.segments.is_empty(), "Empty flush => no new segment");

        // Now flush with data
        let mut mem = make_memtable(&[
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
        ]);
        seg_mgr
            .flush_memtable_to_segment("coll", &mut mem, 101)
            .unwrap();

        // We expect one new segment
        assert_eq!(seg_mgr.segments.len(), 1, "Should have 1 new segment");
        let seg = &seg_mgr.segments[0];
        assert_eq!(seg.start_height, 101);
        assert_eq!(seg.end_height, 101);
        assert!(
            seg.index.is_some(),
            "Should have an in‐memory BTree index loaded"
        );

        // The .seg file and .idx file should exist on disk
        let seg_file_name = format!("coll_{}_{}.seg", 101, 101);
        let idx_file_name = format!("coll_{}_{}.idx", 101, 101);
        let seg_path = tmp_dir.path().join(seg_file_name);
        let idx_path = tmp_dir.path().join(idx_file_name);
        assert!(seg_path.exists(), "Segment file must exist");
        assert!(idx_path.exists(), "Index file must exist");

        // The code does not clear the memtable by default.
        assert!(!mem.is_empty(), "Memtable remains non‐empty after flush");
    }

    #[test]
    fn test_find_value_for_key() {
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();
        let mut mem = make_memtable(&[
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
        ]);
        seg_mgr
            .flush_memtable_to_segment("fruits", &mut mem, 50)
            .unwrap();
        // Now the data is in the single segment with block range [50..50]

        // Test an existing key
        let found = seg_mgr.find_value_for_key("fruits", b"apple").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap(), b"red");

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

        // We simulate that we have three segments covering block heights 100, 101..102, and 103
        // We'll do it by manually flushing.

        // 1) flush block 100
        let mut mem_a = make_memtable(&[(b"a1".to_vec(), b"val100".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem_a, 100)
            .unwrap();

        // 2) flush block 101
        let mut mem_b = make_memtable(&[(b"b1".to_vec(), b"val101".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem_b, 101)
            .unwrap();

        // 3) flush block 103
        let mut mem_c = make_memtable(&[(b"c1".to_vec(), b"val103".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem_c, 103)
            .unwrap();

        assert_eq!(seg_mgr.segments.len(), 3);

        // Now let's rollback to height=101. Only block 103 is above 101, so remove that segment.
        seg_mgr.rollback_to_height(101).unwrap();
        assert_eq!(
            seg_mgr.segments.len(),
            2,
            "Should have removed the segment above block 101"
        );

        // The last segment we created was for block 103 => it should be gone
        assert_eq!(seg_mgr.segments[0].start_height, 100);
        assert_eq!(seg_mgr.segments[0].end_height, 100);
        assert_eq!(seg_mgr.segments[1].start_height, 101);
        assert_eq!(seg_mgr.segments[1].end_height, 101);
    }

    #[test]
    fn test_file_cleanup_on_rollback() {
        let (tmp_dir, mut seg_mgr) = make_segment_manager();
        let mut mem_1 = make_memtable(&[(b"one".to_vec(), b"111".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("mycoll", &mut mem_1, 10)
            .unwrap();

        let mut mem_2 = make_memtable(&[(b"two".to_vec(), b"222".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("mycoll", &mut mem_2, 12)
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
        // This test ensures the parallel code in `discover_existing_segments` doesn't crash
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

        // Create a MemTable with one K=[1,2,3], V=[4,5,6]
        let mut mem = make_memtable(&[(vec![1, 2, 3], vec![4, 5, 6])]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem, 50)
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
    fn test_flush_memtable_multiple_times() {
        // Verifies that multiple flushes create multiple segments and the manager handles them properly.
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // Flush #1
        let mut mem_1 = make_memtable(&[(b"hello".to_vec(), b"world".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("coll", &mut mem_1, 10)
            .unwrap();
        assert_eq!(seg_mgr.segments.len(), 1);

        // Flush #2
        let mut mem_2 = make_memtable(&[(b"foo".to_vec(), b"bar".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("coll", &mut mem_2, 11)
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
    fn test_flush_memtable_concurrent_calls() {
        // Check that calling flush concurrently from multiple threads does not corrupt data or panic.
        // We'll spawn N threads, each flushes a different memtable. Because of `flush_lock`, they should
        // effectively serialize, but we ensure the final state is correct.

        use std::thread;

        let (_tmp_dir, seg_mgr) = make_segment_manager();
        let seg_mgr_arc = Arc::new(Mutex::new(seg_mgr));

        let mut handles = vec![];
        for i in 0..5 {
            let seg_mgr_clone = seg_mgr_arc.clone();
            handles.push(thread::spawn(move || {
                let mut mem = make_memtable(&[(
                    format!("key{i}").into_bytes(),
                    format!("val{i}").into_bytes(),
                )]);
                // Acquire a lock on seg_mgr and flush
                let mut guard = seg_mgr_clone.lock().unwrap();
                guard
                    .flush_memtable_to_segment("concurrent", &mut mem, 100 + i as u64)
                    .unwrap();
            }));
        }

        // Join all threads
        for h in handles {
            h.join().unwrap();
        }

        // Now check we have 5 segments in ascending block height order
        let seg_mgr_final = seg_mgr_arc.lock().unwrap();
        assert_eq!(
            seg_mgr_final.segments.len(),
            5,
            "Should have 5 segments from concurrent flush"
        );
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
        let mut mem = make_memtable(&[(b"cat".to_vec(), b"meow".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem, 10)
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
        let mut mem = make_memtable(&[(b"abc".to_vec(), b"def".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("test", &mut mem, 1)
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
        // 1) flush multiple large memtables,
        // 2) read random keys back.

        // Adjust these for your desired test scale
        const NUM_FLUSHES: usize = 5;
        const ITEMS_PER_FLUSH: usize = 100_000;

        // Create fresh SegmentManager
        let (_tmp_dir, mut seg_mgr) = make_segment_manager();

        // 1) Measure the time to flush multiple memtables
        let start_flush = Instant::now();
        for flush_index in 0..NUM_FLUSHES {
            let mut data = Vec::with_capacity(ITEMS_PER_FLUSH);
            for i in 0..ITEMS_PER_FLUSH {
                // Key and value as bytes
                let key = format!("key-{}-{}", flush_index, i).into_bytes();
                let val = format!("val-{}-{}", flush_index, i).into_bytes();
                data.push((key, val));
            }
            let mut memtable = make_memtable(&data);
            seg_mgr
                .flush_memtable_to_segment("perf_test", &mut memtable, 1000 + flush_index as u64)
                .expect("flush memtable failed");
        }
        let flush_duration = start_flush.elapsed();
        println!(
            "Flushed {} memtables of {} items each in: {:?}",
            NUM_FLUSHES, ITEMS_PER_FLUSH, flush_duration
        );

        // 2) Measure the time to read random keys from the segments
        use rand::{rng, Rng};
        let mut rng = rng();
        let start_read = Instant::now();
        // We'll do some random lookups
        let lookups = 50000;
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
        // 1) Create a SegmentManager and flush a MemTable to produce a .seg and .idx on disk
        let (tmp_dir, mut seg_mgr) = make_segment_manager();

        // Write out a memtable with a single key-value pair
        let mut memtable = make_memtable(&[(b"discovery_key".to_vec(), b"discovery_val".to_vec())]);
        seg_mgr
            .flush_memtable_to_segment("disc", &mut memtable, 42)
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
        //    which demonstrates that `BTreeIndex::read_from_disk` actually returned Ok(btree).
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

use crate::domain::generic::errors::OpNetResult;
use crate::domain::io::{ByteReader, ByteWriter};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct SegmentIndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

pub struct SegmentIndexFile {
    entries: Vec<SegmentIndexEntry>,
}

impl SegmentIndexFile {
    pub fn new() -> Self {
        SegmentIndexFile {
            entries: Vec::new(),
        }
    }

    /// Build the in-memory index from a list of (key, offset) pairs.
    /// We then sort by key.
    pub fn build(&mut self, raw_pairs: Vec<(Vec<u8>, u64)>) {
        self.entries = raw_pairs
            .into_iter()
            .map(|(k, o)| SegmentIndexEntry { key: k, offset: o })
            .collect();

        // Sort by the key bytes lexicographically
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));
    }

    /// Write the index to disk as a separate file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> OpNetResult<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let mut writer = ByteWriter::new(BufWriter::new(file));

        // 1) Write the number of entries
        writer.write_u64(self.entries.len() as u64)?;

        // 2) For each entry, write var_bytes(key) + offset
        for entry in &self.entries {
            writer.write_var_bytes(&entry.key)?;
            writer.write_u64(entry.offset)?;
        }

        writer.flush()?;

        Ok(())
    }

    /// Load the index from a file on disk into memory
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> OpNetResult<Self> {
        let file = File::open(path)?;
        let mut reader = ByteReader::new(BufReader::new(file));

        let len = reader.read_u64()? as usize;
        let mut entries = Vec::with_capacity(len);

        for _ in 0..len {
            let key = reader.read_var_bytes()?;
            let offset = reader.read_u64()?;
            entries.push(SegmentIndexEntry { key, offset });
        }

        Ok(SegmentIndexFile { entries })
    }

    /// Locate the offset for a given key using a binary search in memory.
    pub fn find_offset(&self, search_key: &[u8]) -> Option<u64> {
        // Because self.entries is sorted by 'key', we can do a standard binary search
        let mut low = 0usize;
        let mut high = self.entries.len();

        while low < high {
            let mid = (low + high) / 2;
            let cmp = self.entries[mid].key.as_slice().cmp(search_key);
            match cmp {
                std::cmp::Ordering::Equal => return Some(self.entries[mid].offset),
                std::cmp::Ordering::Less => {
                    low = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self};
    use std::io::Write;
    use std::path::PathBuf;

    /// A helper function to create a temporary file path for testing.
    /// It ensures that each test can operate on a unique file.
    fn get_temp_file_path(test_name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("test_segment_index_{}", test_name));
        dir
    }

    /// Test building the index with an empty list of pairs.
    #[test]
    fn test_build_empty() {
        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(vec![]);

        assert_eq!(segment_index.entries.len(), 0);
    }

    /// Test building the index with multiple (key, offset) pairs and ensuring it is sorted.
    #[test]
    fn test_build_non_empty() {
        let mut segment_index = SegmentIndexFile::new();

        let pairs = vec![
            (b"banana".to_vec(), 20),
            (b"apple".to_vec(), 10),
            (b"cherry".to_vec(), 30),
        ];

        segment_index.build(pairs);

        // The sort order should be: "apple", "banana", "cherry"
        assert_eq!(segment_index.entries.len(), 3);
        assert_eq!(segment_index.entries[0].key, b"apple".to_vec());
        assert_eq!(segment_index.entries[0].offset, 10);
        assert_eq!(segment_index.entries[1].key, b"banana".to_vec());
        assert_eq!(segment_index.entries[1].offset, 20);
        assert_eq!(segment_index.entries[2].key, b"cherry".to_vec());
        assert_eq!(segment_index.entries[2].offset, 30);
    }

    /// Test find_offset on empty index.
    #[test]
    fn test_find_offset_empty_index() {
        let segment_index = SegmentIndexFile::new();
        assert_eq!(segment_index.find_offset(b"any_key"), None);
    }

    /// Test find_offset with existing and missing keys in a small index.
    #[test]
    fn test_find_offset() {
        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(vec![(b"key1".to_vec(), 42), (b"key2".to_vec(), 100)]);

        assert_eq!(segment_index.find_offset(b"key1"), Some(42));
        assert_eq!(segment_index.find_offset(b"key2"), Some(100));
        assert_eq!(segment_index.find_offset(b"missing"), None);
    }

    /// Test find_offset when multiple keys share a prefix but differ in some trailing bytes.
    #[test]
    fn test_find_offset_prefix_variations() {
        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(vec![
            (b"abc".to_vec(), 10),
            (b"abcd".to_vec(), 20),
            (b"abcde".to_vec(), 30),
        ]);

        // Should match exactly if the key is fully matched.
        assert_eq!(segment_index.find_offset(b"abc"), Some(10));
        assert_eq!(segment_index.find_offset(b"abcd"), Some(20));
        assert_eq!(segment_index.find_offset(b"abcde"), Some(30));

        // Partial prefix matches should fail if the exact key does not exist.
        assert_eq!(segment_index.find_offset(b"ab"), None);
        assert_eq!(segment_index.find_offset(b"abcdef"), None);
    }

    /// Test writing and reading a non-empty index to/from file, then ensuring the
    /// loaded data matches the original.
    #[test]
    fn test_write_and_load_from_file() {
        let test_file = get_temp_file_path("write_and_load");
        // Remove if something already exists at this path.
        let _ = fs::remove_file(&test_file);

        let mut segment_index = SegmentIndexFile::new();
        let pairs = vec![
            (b"apple".to_vec(), 10),
            (b"banana".to_vec(), 20),
            (b"cherry".to_vec(), 30),
        ];
        segment_index.build(pairs);

        // Write to file
        segment_index
            .write_to_file(&test_file)
            .expect("Failed to write to file");

        // Load from file
        let loaded_index =
            SegmentIndexFile::load_from_file(&test_file).expect("Failed to load from file");

        // Check that loaded data matches the original
        assert_eq!(loaded_index.entries.len(), 3);
        assert_eq!(loaded_index.entries[0].key, b"apple".to_vec());
        assert_eq!(loaded_index.entries[0].offset, 10);
        assert_eq!(loaded_index.entries[1].key, b"banana".to_vec());
        assert_eq!(loaded_index.entries[1].offset, 20);
        assert_eq!(loaded_index.entries[2].key, b"cherry".to_vec());
        assert_eq!(loaded_index.entries[2].offset, 30);

        // Clean up
        let _ = fs::remove_file(&test_file);
    }

    /// Test writing and loading an empty index file.
    #[test]
    fn test_write_and_load_empty() {
        let test_file = get_temp_file_path("write_and_load_empty");
        let _ = fs::remove_file(&test_file);

        let segment_index = SegmentIndexFile::new(); // no entries

        // Write to file
        segment_index
            .write_to_file(&test_file)
            .expect("Failed to write empty file");

        // Load from file
        let loaded_index =
            SegmentIndexFile::load_from_file(&test_file).expect("Failed to load empty file");

        assert_eq!(loaded_index.entries.len(), 0);

        let _ = fs::remove_file(&test_file);
    }

    /// Test load_from_file with a non-existent file should return an error.
    #[test]
    fn test_load_from_nonexistent_file() {
        let test_file = get_temp_file_path("nonexistent_index_file");
        let _ = fs::remove_file(&test_file);

        let result = SegmentIndexFile::load_from_file(&test_file);
        // We expect an Err variant (std::io::Error)
        assert!(result.is_err());
    }

    /// Test loading a file that is truncated/corrupted (e.g., missing some bytes).
    /// This simulates a file that begins normally, but is missing a partial entry.
    #[test]
    fn test_load_from_corrupted_file() {
        let test_file = get_temp_file_path("corrupted_file");
        let _ = fs::remove_file(&test_file);

        // Build a legitimate index first
        {
            let mut segment_index = SegmentIndexFile::new();
            segment_index.build(vec![(b"key".to_vec(), 42)]);
            segment_index
                .write_to_file(&test_file)
                .expect("Failed to write index");
        }

        // Now open the file and truncate some bytes
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&test_file)
                .expect("Failed to open file for truncation");

            // We will cut off a few bytes from the end by setting the file length
            // to half of its current size (just for demonstration).
            let metadata = file.metadata().expect("Failed to get metadata");
            let new_len = metadata.len() / 2;
            file.set_len(new_len).expect("Failed to truncate file");
        }

        // Now try loading the corrupted file
        let result = SegmentIndexFile::load_from_file(&test_file);
        assert!(result.is_err(), "Loading a corrupted file should fail.");

        // Clean up
        let _ = fs::remove_file(&test_file);
    }

    /// Test that find_offset still works with multiple entries
    /// after round-trip (write -> load).
    #[test]
    fn test_find_offset_after_round_trip() {
        let test_file = get_temp_file_path("find_offset_after_round_trip");
        let _ = fs::remove_file(&test_file);

        // Build index
        let mut original_index = SegmentIndexFile::new();
        original_index.build(vec![
            (b"alpha".to_vec(), 1000),
            (b"bravo".to_vec(), 2000),
            (b"charlie".to_vec(), 3000),
        ]);

        // Write
        original_index
            .write_to_file(&test_file)
            .expect("Failed to write index");

        // Load
        let loaded_index =
            SegmentIndexFile::load_from_file(&test_file).expect("Failed to load index");

        // Now test searching for offsets
        assert_eq!(loaded_index.find_offset(b"alpha"), Some(1000));
        assert_eq!(loaded_index.find_offset(b"bravo"), Some(2000));
        assert_eq!(loaded_index.find_offset(b"charlie"), Some(3000));
        assert_eq!(loaded_index.find_offset(b"nonexistent"), None);

        let _ = fs::remove_file(&test_file);
    }

    /// Test building the index with repeated keys. Ensure that the index sorting still works
    /// and that we preserve the order. (In a real usage scenario, repeated keys might be
    /// disallowed or you might want the latest offset, etc., but let's just confirm behavior.)
    #[test]
    fn test_build_with_duplicate_keys() {
        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(vec![(b"dup".to_vec(), 42), (b"dup".to_vec(), 100)]);
        // The final order is stable based on the key, but since the keys are identical,
        // it will not reorder them by offset. However, we test the presence of both.
        assert_eq!(segment_index.entries.len(), 2);
        // They are sorted lexicographically by key (the same key),
        // so their order depends on stable sorting for duplicates.
        // By default, .sort_by() with the same key won't reorder them, but this is
        // an implementation detail. We can at least check that both exist.
        assert_eq!(segment_index.entries[0].key, b"dup");
        assert_eq!(segment_index.entries[0].offset, 42);
        assert_eq!(segment_index.entries[1].key, b"dup");
        assert_eq!(segment_index.entries[1].offset, 100);
    }

    /// Test writing index with duplicate keys and ensure it can be loaded properly.
    #[test]
    fn test_write_and_load_with_duplicate_keys() {
        let test_file = get_temp_file_path("write_and_load_dup_keys");
        let _ = fs::remove_file(&test_file);

        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(vec![
            (b"apple".to_vec(), 10),
            (b"apple".to_vec(), 20),
            (b"banana".to_vec(), 30),
        ]);

        segment_index
            .write_to_file(&test_file)
            .expect("Failed to write dup-keys file");

        let loaded_index =
            SegmentIndexFile::load_from_file(&test_file).expect("Failed to load dup-keys file");

        // All entries should be present.
        assert_eq!(loaded_index.entries.len(), 3);
        // The sort is lexicographic by 'apple' => 'apple' => 'banana'
        assert_eq!(loaded_index.entries[0].key, b"apple");
        assert_eq!(loaded_index.entries[0].offset, 10);
        assert_eq!(loaded_index.entries[1].key, b"apple");
        assert_eq!(loaded_index.entries[1].offset, 20);
        assert_eq!(loaded_index.entries[2].key, b"banana");
        assert_eq!(loaded_index.entries[2].offset, 30);

        let _ = fs::remove_file(&test_file);
    }

    /// Demonstrates writing a large number of entries and ensuring they
    /// can be loaded and searched. Helps test performance as well as correctness.
    #[test]
    fn test_large_index() {
        let mut tmp_file = tempfile::NamedTempFile::new();
        let test_file = tmp_file.unwrap().into_temp_path();

        let mut raw_pairs = Vec::new();
        // Insert 1000 entries for demonstration (could be more, but let's keep tests fast).
        for i in 0..1000 {
            // Create a key like "keyXXXXX"
            let key = format!("key{:04}", i).into_bytes();
            raw_pairs.push((key, i as u64));
        }

        let mut segment_index = SegmentIndexFile::new();
        segment_index.build(raw_pairs);

        // Write
        segment_index
            .write_to_file(&test_file)
            .expect("Failed to write large index");

        // Load
        let loaded_index = SegmentIndexFile::load_from_file(&test_file)
            .expect("OpNetError: Failed to load large index");

        // Check a few random offsets
        assert_eq!(loaded_index.find_offset(b"key0000"), Some(0));
        assert_eq!(loaded_index.find_offset(b"key0025"), Some(25));
        assert_eq!(loaded_index.find_offset(b"key0999"), Some(999));
        // Missing key
        assert_eq!(loaded_index.find_offset(b"key1001"), None);

        let _ = fs::remove_file(&test_file);
    }
}

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

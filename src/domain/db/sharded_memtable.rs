use std::collections::HashMap;
use std::sync::Mutex;

/// A single shard of the memtable.
/// Holds a HashMap for key/value pairs plus a small accounting of the current size.
pub struct MemTableShard {
    pub(crate) data: HashMap<Vec<u8>, Vec<u8>>,
    current_size: usize,
}

impl MemTableShard {
    /// Create a new, empty shard.
    pub fn new() -> Self {
        MemTableShard {
            data: HashMap::new(),
            current_size: 0,
        }
    }

    /// Insert or update a key in this shard.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // If key already exists, remove the old value from current_size
        if let Some(old_val) = self.data.get(&key) {
            self.current_size = self.current_size.saturating_sub(old_val.len());
        }

        self.current_size += value.len();
        self.data.insert(key, value);
    }

    /// Retrieve a value for a given key.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(|v| &v[..])
    }

    /// Clear everything in this shard.
    pub fn clear(&mut self) {
        self.data.clear();
        self.current_size = 0;
    }

    /// Returns how many bytes of user data are stored in this shard.
    pub fn size(&self) -> usize {
        self.current_size
    }

    /// Returns how many entries are in this shard.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this shard is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// A sharded memtable that splits keys across multiple shards (buckets).
///
/// Each shard is protected by its own `Mutex` so inserts/gets on different shards
/// can run in parallel.
pub struct ShardedMemTable {
    // Each shard has its own lock + data
    pub(crate) shards: Vec<Mutex<MemTableShard>>,
    // Typically a power of 2, but can be any positive integer
    shard_count: usize,
    // Maximum total in-memory size across all shards before flush is triggered
    max_size: usize,
}

impl ShardedMemTable {
    /// Create a new ShardedMemTable with `shard_count` shards.
    /// The `max_size` is a global threshold across all shards.
    pub fn new(shard_count: usize, max_size: usize) -> Self {
        assert!(shard_count > 0, "Need at least one shard.");
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(MemTableShard::new()));
        }
        ShardedMemTable {
            shards,
            shard_count,
            max_size,
        }
    }

    /// Insert a key-value pair into the appropriate shard.
    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard_id = self.shard_for_key(&key);
        // Lock only the relevant shard
        let mut shard_guard = self.shards[shard_id].lock().unwrap();
        shard_guard.insert(key, value);
    }

    /// Get a value from the appropriate shard.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        let shard_guard = self.shards[shard_id].lock().unwrap();
        shard_guard.get(key).map(|v| v.to_vec())
    }

    /// Clear all shards.
    pub fn clear_all(&self) {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            guard.clear();
        }
    }

    /// Returns the total approximate in-memory size across all shards.
    /// This is used to check if we exceed `max_size`.
    pub fn total_size(&self) -> usize {
        let mut sum = 0;
        for shard in &self.shards {
            let guard = shard.lock().unwrap();
            sum += guard.size();
        }
        sum
    }

    /// Returns the configured maximum size at which we want to flush.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Helper to pick which shard ID a key goes to.
    /// Simple approach: sum of bytes mod shard_count or any better hash.
    fn shard_for_key(&self, key: &[u8]) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }
}

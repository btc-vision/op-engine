use std::collections::HashMap;

pub struct MemTable {
    pub data: HashMap<Vec<u8>, Vec<u8>>,
    max_size: usize,
    current_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        MemTable {
            data: HashMap::new(),
            max_size,
            current_size: 0,
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // If key already exists, remove the old value from current_size
        if let Some(old_val) = self.data.get(&key) {
            self.current_size = self.current_size.saturating_sub(old_val.len());
        }

        self.current_size += value.len();
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.data.get(key).map(|v| &v[..])
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.current_size = 0;
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

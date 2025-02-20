use std::collections::HashMap;

/// A map of key -> offset
#[derive(Default)]
pub struct SegmentIndex {
    pub map: HashMap<Vec<u8>, u64>,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, offset: u64) {
        self.map.insert(key, offset);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

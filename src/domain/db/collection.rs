use crate::domain::blockchain::reorg::ReorgManager;
use crate::domain::db::memtable::MemTable;
use crate::domain::db::segments::segment::SegmentManager;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::db::wal::WAL;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone, Debug)]
pub struct CollectionMetadata {
    pub name: String,
}

impl CollectionMetadata {
    pub fn new(name: &str) -> Self {
        Self { name: name.into() }
    }
}

pub struct Collection<T>
where
    T: KeyProvider,
{
    name: String,
    memtables: Arc<RwLock<HashMap<String, MemTable>>>,
    wal: Arc<Mutex<WAL>>,
    segment_manager: Arc<Mutex<SegmentManager>>,
    reorg_manager: Arc<Mutex<ReorgManager>>,
    metadata: CollectionMetadata,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Collection<T>
where
    T: KeyProvider,
{
    pub fn new(
        name: String,
        memtables: Arc<RwLock<HashMap<String, MemTable>>>,
        wal: Arc<Mutex<WAL>>,
        segment_manager: Arc<Mutex<SegmentManager>>,
        reorg_manager: Arc<Mutex<ReorgManager>>,
        metadata: CollectionMetadata,
    ) -> Self {
        Collection {
            name,
            memtables,
            wal,
            segment_manager,
            reorg_manager,
            metadata,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Insert a record
    pub fn insert(&self, record: T, block_height: u64) -> OpNetResult<()> {
        let mut buffer = Vec::new();
        {
            let mut writer = ByteWriter::new(&mut buffer);
            record.serialize(&mut writer)?;
        }
        let key = record.primary_key();

        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(&buffer)?;
        }

        {
            let mut mem_guard = self.memtables.write().unwrap();
            let memtable = mem_guard
                .get_mut(&self.name)
                .ok_or_else(|| OpNetError::new("No memtable for collection"))?;
            memtable.insert(key, buffer);

            if memtable.current_size() > memtable.max_size() {
                // flush
                let mut segmgr = self.segment_manager.lock().unwrap();
                segmgr.flush_memtable_to_segment(&self.name, memtable, block_height)?;
                memtable.clear();
            }
        }

        Ok(())
    }

    /// Get by key args
    pub fn get(&self, key_args: &T::KeyArgs) -> OpNetResult<Option<T>> {
        let key = T::compose_key(key_args);

        {
            let mem_guard = self.memtables.read().unwrap();
            if let Some(memtable) = mem_guard.get(&self.name) {
                if let Some(raw) = memtable.get(&key) {
                    let mut reader = ByteReader::new(&raw[..]);
                    let obj = T::deserialize(&mut reader)?;
                    return Ok(Some(obj));
                }
            }
        }

        {
            let segmgr = self.segment_manager.lock().unwrap();
            if let Some(raw) = segmgr.find_value_for_key(&self.name, &key)? {
                let mut reader = ByteReader::new(&raw[..]);
                let obj = T::deserialize(&mut reader)?;
                return Ok(Some(obj));
            }
        }

        Ok(None)
    }
}

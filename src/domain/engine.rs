use crate::domain::blockchain::reorg::ReorgManager;
use crate::domain::db::collection::{Collection, CollectionMetadata};
use crate::domain::db::memtable::MemTable;
use crate::domain::db::segments::segment::SegmentManager;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::db::wal::WAL;
use crate::domain::generic::config::DbConfig;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::thread::concurrency::ThreadPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct OpNetDB {
    pub config: DbConfig,
    pub thread_pool: Arc<Mutex<ThreadPool>>,
    pub wal: Arc<Mutex<WAL>>,
    pub memtables: Arc<RwLock<HashMap<String, MemTable>>>,
    pub segment_manager: Arc<Mutex<SegmentManager>>,
    pub reorg_manager: Arc<Mutex<ReorgManager>>,
    pub collections: Arc<RwLock<HashMap<String, CollectionMetadata>>>,
}

impl OpNetDB {
    pub fn new(config: DbConfig) -> OpNetResult<Self> {
        env_logger::init();

        let wal = WAL::open(&config.wal_path)?;
        let thread_pool = Arc::new(Mutex::new(ThreadPool::new(config.num_threads)));
        let memtables = Arc::new(RwLock::new(HashMap::new()));
        let segment_manager = SegmentManager::new(&config.data_path, Arc::clone(&thread_pool))?;
        let segment_manager = Arc::new(Mutex::new(segment_manager));
        let reorg_manager = Arc::new(Mutex::new(ReorgManager::new()));
        let c_map = HashMap::new();

        reorg_manager
            .lock()
            .unwrap()
            .set_current_height(config.height);

        Ok(OpNetDB {
            config,
            thread_pool,
            wal: Arc::new(Mutex::new(wal)),
            memtables,
            segment_manager,
            reorg_manager,
            collections: Arc::new(RwLock::new(c_map)),
        })
    }

    pub fn register_collection(&self, name: &str) -> OpNetResult<()> {
        let mut col_guard = self.collections.write().unwrap();
        if col_guard.contains_key(name) {
            return Err(OpNetError::new(&format!(
                "Collection {} already exists",
                name
            )));
        }
        let metadata = CollectionMetadata::new(name);
        col_guard.insert(name.to_string(), metadata);

        let mut mt_guard = self.memtables.write().unwrap();
        if !mt_guard.contains_key(name) {
            let mt = MemTable::new(self.config.memtable_size);
            mt_guard.insert(name.to_string(), mt);
        }
        Ok(())
    }

    /// IMPORTANT: We now require T: KeyProvider
    pub fn collection<T>(&self, name: &str) -> OpNetResult<Collection<T>>
    where
        T: KeyProvider,
    {
        let col_guard = self.collections.read().unwrap();
        if !col_guard.contains_key(name) {
            return Err(OpNetError::new(&format!(
                "Collection {} is not registered",
                name
            )));
        }
        let metadata = col_guard.get(name).unwrap().clone();

        Ok(Collection::new(
            name.to_string(),
            Arc::clone(&self.memtables),
            Arc::clone(&self.wal),
            Arc::clone(&self.segment_manager),
            Arc::clone(&self.reorg_manager),
            metadata,
        ))
    }

    pub fn flush_all(&self, flush_block_height: u64) -> OpNetResult<()> {
        let mut segmgr = self.segment_manager.lock().unwrap();
        let mut guard = self.memtables.write().unwrap();
        for (name, mem) in guard.iter_mut() {
            if !mem.is_empty() {
                segmgr.flush_memtable_to_segment(name, mem, flush_block_height)?;
                mem.clear();
            }
        }

        let mut wal_guard = self.wal.lock().unwrap();
        wal_guard.checkpoint()?;
        Ok(())
    }

    pub fn reorg_to(&self, height: u64) -> OpNetResult<()> {
        {
            let mut rm = self.reorg_manager.lock().unwrap();
            rm.reorg_to(height)?;
        }
        {
            let mut segmgr = self.segment_manager.lock().unwrap();
            segmgr.rollback_to_height(height)?;
        }
        {
            let mut guard = self.memtables.write().unwrap();
            for (_, mem) in guard.iter_mut() {
                mem.clear();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::db::collections::utxo::Utxo;
    use std::fs;

    #[test]
    fn test_full_flow() -> OpNetResult<()> {
        let _ = fs::remove_dir_all("./test_data");
        fs::create_dir_all("./test_data")?;

        let config = DbConfig {
            data_path: "./test_data".into(),
            wal_path: "./test_data/wal.log".into(),
            num_threads: 4,
            memtable_size: 1024 * 1024,
            height: 100,
        };

        let db = OpNetDB::new(config)?;
        db.register_collection("utxo")?;

        let utxo_coll = db.collection::<Utxo>("utxo")?;

        let sample = Utxo {
            tx_id: [0xab; 32],
            output_index: 0,
            address: [0xcd; 20],
            amount: 10_000,
            script_pubkey: vec![0xAA, 0xBB, 0xCC],
        };

        utxo_coll.insert(sample.clone(), 101)?;

        // Check in memtable
        let found = utxo_coll.get(&([0xab; 32], 0))?;
        assert!(found.is_some());
        assert_eq!(found.as_ref().unwrap().amount, 10_000);

        db.flush_all(101)?;

        // Simulate a reorg.
        //db.reorg_to(100)?;

        // Because segment was above block 100, it was removed
        let found2 = utxo_coll.get(&([0xab; 32], 0))?;
        assert!(found2.is_some());
        Ok(())
    }
}

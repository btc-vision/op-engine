use crate::domain::blockchain::reorg::ReorgManager;
use crate::domain::db::collection::{Collection, CollectionMetadata};
use crate::domain::db::segments::segment::SegmentManager;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::db::wal::WAL;
use crate::domain::generic::config::DbConfig;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::thread::concurrency::ThreadPool;

use crate::domain::db::sharded_memtable::ShardedMemTable;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct OpNetDB {
    pub config: DbConfig,
    pub thread_pool: Arc<Mutex<ThreadPool>>,
    pub wal: Arc<Mutex<WAL>>,
    pub sharded_tables: Arc<RwLock<HashMap<String, ShardedMemTable>>>,
    pub segment_manager: Arc<Mutex<SegmentManager>>,
    pub reorg_manager: Arc<Mutex<ReorgManager>>,
    pub collections: Arc<RwLock<HashMap<String, CollectionMetadata>>>,
}

impl OpNetDB {
    pub fn new(config: DbConfig) -> OpNetResult<Self> {
        let wal = WAL::open(&config.wal_path)?;
        let thread_pool = Arc::new(Mutex::new(ThreadPool::new(config.num_threads)));

        // Create an empty map of collection_name -> ShardedMemTable
        let sharded_tables = Arc::new(RwLock::new(HashMap::new()));

        let segment_manager = SegmentManager::new(&config.data_path, Arc::clone(&thread_pool))?;
        let segment_manager = Arc::new(Mutex::new(segment_manager));
        let reorg_manager = Arc::new(Mutex::new(ReorgManager::new()));
        reorg_manager
            .lock()
            .unwrap()
            .set_current_height(config.height);

        let c_map = HashMap::new();

        Ok(OpNetDB {
            config,
            thread_pool,
            wal: Arc::new(Mutex::new(wal)),
            sharded_tables,
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

        // Create a ShardedMemTable for this new collection
        let mut st_guard = self.sharded_tables.write().unwrap();
        if !st_guard.contains_key(name) {
            // You can choose shard_count and max_size any way you like
            let shard_count = 8; // example
            let sharded_mem = ShardedMemTable::new(shard_count, self.config.memtable_size);
            st_guard.insert(name.to_string(), sharded_mem);
        }
        Ok(())
    }

    /// Returns a collection handle that references the sharded memtable,
    /// the WAL, the SegmentManager, etc.
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

        // We pass in sharded_tables instead of memtables
        Ok(Collection::new(
            name.to_string(),
            Arc::clone(&self.sharded_tables),
            Arc::clone(&self.wal),
            Arc::clone(&self.segment_manager),
            Arc::clone(&self.reorg_manager),
            metadata,
        ))
    }

    /// Flush *all* the sharded memtables to segments, then checkpoint WAL.
    pub fn flush_all(&self, flush_block_height: u64) -> OpNetResult<()> {
        let mut segmgr = self.segment_manager.lock().unwrap();

        let mut map_guard = self.sharded_tables.write().unwrap();
        for (name, sharded_mem) in map_guard.iter() {
            // If this memtable is non-empty (total_size > 0), flush it
            if sharded_mem.total_size() > 0 {
                segmgr.flush_sharded_memtable_to_segment(name, sharded_mem, flush_block_height)?;
                sharded_mem.clear_all();
            }
        }

        // WAL checkpoint
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
            // Clear all shards in memory
            let mut map_guard = self.sharded_tables.write().unwrap();
            for (_, sharded_mem) in map_guard.iter() {
                sharded_mem.clear_all();
            }
        }
        Ok(())
    }
}

pub fn main() -> () {
    env_logger::init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::db::__test__::helpper::{make_test_config, setup_fs, teardown_fs};
    use crate::domain::db::collections::utxo::Utxo;
    use crate::domain::generic::errors::OpNetResult;
    use std::time::Instant;

    // ------------------------------------------------------------------
    // 1) Basic full-flow test (uses blocks near 101)
    // ------------------------------------------------------------------
    #[test]
    fn test_full_flow() -> OpNetResult<()> {
        let test_name = "full_flow";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 100);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        let sample = Utxo {
            tx_id: [0xab; 32],
            output_index: 0,
            address: [0xcd; 33],
            amount: 10_000,
            script_pubkey: vec![0xAA, 0xBB, 0xCC],
            deleted_at_block: None,
        };

        // Insert at block 101
        utxo_coll.insert(sample.clone(), 101)?;
        let found = utxo_coll.get(&([0xab; 32], 0))?;
        assert!(found.is_some());
        assert_eq!(found.as_ref().unwrap().amount, 10_000);

        // Flush
        db.flush_all(101)?;

        // Confirm data is still there after flush
        let found2 = utxo_coll.get(&([0xab; 32], 0))?;
        assert!(found2.is_some());
        assert_eq!(found2.as_ref().unwrap().amount, 10_000);

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 2) Edge Case: Register a collection that already exists
    // ------------------------------------------------------------------
    #[test]
    fn test_register_existing_collection() -> OpNetResult<()> {
        let test_name = "register_existing_collection";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 100);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        // Trying again should fail
        let result = db.register_collection("utxo");
        assert!(result.is_err());

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 3) Edge Case: Request a collection that doesn't exist
    // ------------------------------------------------------------------
    #[test]
    fn test_collection_not_registered() -> OpNetResult<()> {
        let test_name = "collection_not_registered";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 100);
        let db = OpNetDB::new(config)?;

        // We never registered "utxo"
        let result = db.collection::<Utxo>("utxo");
        assert!(result.is_err());

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 4) Flush all with empty memtables (should be no-op, no errors).
    // ------------------------------------------------------------------
    #[test]
    fn test_flush_empty() -> OpNetResult<()> {
        let test_name = "flush_empty";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 100);
        let db = OpNetDB::new(config)?;

        // Register but don't insert anything
        db.register_collection("utxo")?;
        // Should not panic or error even though memtable is empty
        db.flush_all(101)?;

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 5) Reorg to a past height: from 105 down to 100
    // ------------------------------------------------------------------
    #[test]
    fn test_reorg_past() -> OpNetResult<()> {
        let test_name = "reorg_past";
        setup_fs(test_name)?;

        // Start the chain at 105 so reorg down to 100 is valid
        let config = make_test_config(test_name, 105);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        let sample = Utxo {
            tx_id: [0xcd; 32],
            output_index: 2,
            address: [0xde; 33],
            amount: 5000,
            script_pubkey: vec![0xAA],
            deleted_at_block: None,
        };

        // Insert at height 105
        utxo_coll.insert(sample.clone(), 105)?;
        db.flush_all(105)?;

        // Now reorg back to 100
        db.reorg_to(100)?;

        // Because the segment for data at 105 is above 100, it should be rolled back
        let found = utxo_coll.get(&([0xcd; 32], 2))?;
        assert!(found.is_none(), "Should be removed after reorg");

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 6) Reorg to a future height (above current), which should fail.
    // ------------------------------------------------------------------
    #[test]
    fn test_reorg_future() -> OpNetResult<()> {
        let test_name = "reorg_future";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 105);
        let db = OpNetDB::new(config)?;

        // Attempt reorg to 200
        let result = db.reorg_to(200);
        assert!(
            result.is_err(),
            "Expected error when reorging above current height"
        );

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 7) Flush with big blocks (stress test).
    //    Insert 50,000 UTXOs at block 110, flush, do a spot check.
    // ------------------------------------------------------------------
    #[test]
    fn test_flush_with_big_blocks() -> OpNetResult<()> {
        let test_name = "flush_big_blocks";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 120);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        // Insert 50,000 UTXOs as an example "big" test
        let n = 50_000;
        let start = Instant::now();
        for i in 0..n {
            let mut tx_id = [0u8; 32];
            tx_id[0] = (i % 256) as u8; // only the first byte changes
            tx_id[1] = (i >> 8) as u8;
            tx_id[2] = (i >> 16) as u8;

            let sample = Utxo {
                tx_id,
                output_index: i,
                address: [0xcd; 33],
                amount: i as u64,
                script_pubkey: vec![0xEE, 0xFF],
                deleted_at_block: None,
            };

            // Insert at block 110
            utxo_coll.insert(sample, 110)?;
        }

        let insert_duration = start.elapsed();
        println!("Inserted {} UTXOs in {:?}", n, insert_duration);

        // Flush memtable to disk
        let flush_start = Instant::now();
        db.flush_all(110)?;

        let flush_duration = flush_start.elapsed();
        println!("Flushed {} UTXOs in {:?}", n, flush_duration);

        let loaded = db.collection::<Utxo>("utxo")?;

        // Spot check i=999
        let mut check_key = [0u8; 32];
        check_key[0] = (999 % 256) as u8; // 231
        check_key[1] = (999 >> 8) as u8;
        check_key[2] = (999 >> 16) as u8;

        let key = (check_key, 999);
        let found = loaded.get(&key)?;
        assert!(
            found.is_some(),
            "Expected to find UTXO with i=999 after flush"
        );

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 10) Double-flush and reorg
    // ------------------------------------------------------------------
    #[test]
    fn test_double_flush_and_reorg() -> OpNetResult<()> {
        let test_name = "double_flush_and_reorg";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 120);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        // Insert at block 110, flush
        let sample1 = Utxo {
            tx_id: [1u8; 32],
            output_index: 0,
            address: [0xcd; 33],
            amount: 1_000,
            script_pubkey: vec![0x01],
            deleted_at_block: None,
        };

        utxo_coll.insert(sample1.clone(), 110)?;
        db.flush_all(110)?;

        // Insert at block 111, flush
        let sample2 = Utxo {
            tx_id: [2u8; 32],
            output_index: 1,
            address: [0xcd; 33],
            amount: 2_000,
            script_pubkey: vec![0x02],
            deleted_at_block: None,
        };

        utxo_coll.insert(sample2.clone(), 111)?;
        db.flush_all(111)?;

        // Both should be present now
        let found1 = utxo_coll.get(&([1u8; 32], 0))?;
        assert!(found1.is_some(), "sample1 should be found");
        let found2 = utxo_coll.get(&([2u8; 32], 1))?;
        assert!(found2.is_some(), "sample2 should be found");

        // Reorg back to 110 -> The data at block 111 should be rolled back
        db.reorg_to(110)?;

        // sample1 should remain, sample2 should be removed
        let found1_after = utxo_coll.get(&([1u8; 32], 0))?;
        assert!(found1_after.is_some(), "sample1 should remain after reorg");
        let found2_after = utxo_coll.get(&([2u8; 32], 1))?;
        assert!(
            found2_after.is_none(),
            "sample2 should be removed after reorg"
        );

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 8) Big number of UTXOs across multiple flushes
    // ------------------------------------------------------------------
    #[test]
    fn test_many_flushed_segments() -> OpNetResult<()> {
        let test_name = "many_flushed_segments";
        setup_fs(test_name)?;

        // Make the memtable relatively small to force multiple flushes
        let mut config = make_test_config(test_name, 120);
        config.memtable_size = 1024 * 50; // 50 KB for demonstration
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        let large_count = 10_000;
        let chunk_size = 2_000;
        let mut current_block = 110;

        // Insert in multiple waves
        for chunk_start in (0..large_count).step_by(chunk_size) {
            for i in chunk_start..(chunk_start + chunk_size) {
                let mut tx_id = [0u8; 32];
                tx_id[0] = (i % 256) as u8;
                tx_id[1] = (i >> 8) as u8;
                tx_id[2] = (i >> 16) as u8;
                let sample = Utxo {
                    tx_id,
                    output_index: i as u32,
                    address: [0xcd; 33],
                    amount: i as u64,
                    script_pubkey: vec![0x00],
                    deleted_at_block: None,
                };
                utxo_coll.insert(sample, current_block)?;
            }
            // Flush each chunk
            db.flush_all(current_block)?;
            current_block += 1;
        }

        // Spot check the last chunk includes i=9999
        let mut check_key = [0u8; 32];
        check_key[0] = (9999 % 256) as u8; // 15
        check_key[1] = (9999 >> 8) as u8;
        check_key[2] = (9999 >> 16) as u8;
        let found = utxo_coll.get(&(check_key, 9999))?;
        assert!(
            found.is_some(),
            "Expected to find UTXO for i=9999 in the last chunk"
        );

        teardown_fs(test_name);
        Ok(())
    }

    // ------------------------------------------------------------------
    // 9) Concurrency test: multiple threads inserting
    // ------------------------------------------------------------------
    #[test]
    fn test_concurrent_inserts() -> OpNetResult<()> {
        let test_name = "concurrent_inserts";
        setup_fs(test_name)?;

        let config = make_test_config(test_name, 120);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = Arc::new(db.collection::<Utxo>("utxo")?);

        let mut handles = vec![];
        for thread_id in 0..4 {
            let coll_clone = Arc::clone(&utxo_coll);
            handles.push(std::thread::spawn(move || -> OpNetResult<()> {
                for i in 0..1000 {
                    let amount = (thread_id * 1000 + i) as u64;
                    let mut tx_id = [0u8; 32];
                    tx_id[0] = thread_id as u8;
                    tx_id[1] = (i % 256) as u8;

                    let sample = Utxo {
                        tx_id,
                        output_index: i,
                        address: [0xaa; 33],
                        amount,
                        script_pubkey: vec![0xAA, 0xBB],
                        deleted_at_block: None,
                    };
                    // Insert at block 120
                    coll_clone.insert(sample, 120)?;
                }
                Ok(())
            }));
        }

        for h in handles {
            let res = h.join();
            assert!(res.is_ok());
            if let Ok(Err(e)) = res {
                panic!("Thread returned error: {:?}", e);
            }
        }

        // Finally, flush
        db.flush_all(120)?;

        // Spot check
        let mut check_key = [0u8; 32];
        check_key[1] = 250;
        let found = utxo_coll.get(&(check_key, 250))?;
        assert!(
            found.is_some(),
            "Expected to find UTXO for i=250 from thread 0"
        );

        teardown_fs(test_name);
        Ok(())
    }

    #[test]
    fn test_one_hundred_big_blocks_performance() -> OpNetResult<()> {
        let test_name = "one_hundred_big_blocks_perf";
        setup_fs(test_name)?;

        // We'll start the DB at height=500 (arbitrary), so blocks near 600 are valid if we like
        let config = make_test_config(test_name, 500);
        let db = OpNetDB::new(config)?;

        db.register_collection("utxo")?;
        let utxo_coll = db.collection::<Utxo>("utxo")?;

        // Adjust these constants as desired
        const BLOCK_COUNT: usize = 100;
        const ITEMS_PER_BLOCK: usize = 10_000;

        // Start measuring insertion + flushing
        let start_insert = Instant::now();

        for b in 0..BLOCK_COUNT {
            let block_height = 600 + b as u64; // e.g. 600..699
            for i in 0..ITEMS_PER_BLOCK {
                let mut tx_id = [0u8; 32];
                tx_id[0] = b as u8;
                // fill a couple more bytes so we don't all share the same prefix
                tx_id[1] = (i % 256) as u8;
                tx_id[2] = (i >> 8) as u8;

                let sample = Utxo {
                    tx_id,
                    output_index: i as u32,
                    address: [0xcd; 33],
                    amount: i as u64,
                    script_pubkey: vec![0xEE, 0xFF],
                    deleted_at_block: None,
                };
                utxo_coll.insert(sample, block_height)?;
            }
            // Flush after each block
            db.flush_all(block_height)?;
        }

        let insert_duration = start_insert.elapsed();
        println!(
            "Inserted and flushed {} blocks x {} UTXOs each in {:?}",
            BLOCK_COUNT, ITEMS_PER_BLOCK, insert_duration
        );

        // We'll measure how long it takes to retrieve a known record
        // Let's pick block 50, item 1234 for instance
        let target_block = 600 + 50; // block #50
        let target_i = 1234;

        // Compose the same tx_id we used above for b=50 => tx_id[0] = 50, tx_id[1] = 1234 % 256, etc.
        let mut tx_id = [0u8; 32];
        tx_id[0] = 50;
        tx_id[1] = (target_i % 256) as u8;
        tx_id[2] = (target_i >> 8) as u8;

        // Build the key
        let key = (tx_id, target_i as u32);

        let start_lookup = Instant::now();
        let found = utxo_coll.get(&key)?;
        let lookup_duration = start_lookup.elapsed();
        assert!(found.is_some(), "Expected to find UTXO at block=50, i=1234");

        println!(
            "Lookup for block=50, i=1234 took: {:?}, result amount={}",
            lookup_duration,
            found.as_ref().unwrap().amount
        );

        teardown_fs(test_name);
        Ok(())
    }
}

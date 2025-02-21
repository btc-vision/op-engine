// domain/db/collection.rs
use crate::domain::blockchain::reorg::ReorgManager;
use crate::domain::db::segments::segment::SegmentManager;
use crate::domain::db::sharded_memtable::ShardedMemTable;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::db::wal::WAL;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter, CustomSerialize};
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
    pub(crate) name: String,
    /// Instead of a single MemTable per collection, we now have a ShardedMemTable.
    pub(crate) sharded_tables: Arc<RwLock<HashMap<String, ShardedMemTable>>>,
    pub(crate) wal: Arc<Mutex<WAL>>,
    pub(crate) segment_manager: Arc<Mutex<SegmentManager>>,
    pub(crate) reorg_manager: Arc<Mutex<ReorgManager>>,
    pub(crate) metadata: CollectionMetadata,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Collection<T>
where
    T: KeyProvider + CustomSerialize,
{
    /// Construct a new `Collection<T>` that references a HashMap of `String -> ShardedMemTable`.
    /// Typically, you’ll store one `ShardedMemTable` for each collection name.
    pub fn new(
        name: String,
        sharded_tables: Arc<RwLock<HashMap<String, ShardedMemTable>>>,
        wal: Arc<Mutex<WAL>>,
        segment_manager: Arc<Mutex<SegmentManager>>,
        reorg_manager: Arc<Mutex<ReorgManager>>,
        metadata: CollectionMetadata,
    ) -> Self {
        Collection {
            name,
            sharded_tables,
            wal,
            segment_manager,
            reorg_manager,
            metadata,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Insert or update a record in this collection. Uses the sharded memtable under the hood.
    pub fn insert(&self, record: T, block_height: u64) -> OpNetResult<()> {
        // 1) Serialize the record
        let mut buffer = Vec::new();
        {
            let mut writer = ByteWriter::new(&mut buffer);
            record.serialize(&mut writer)?;
        }
        let key = record.primary_key();

        // 2) Append to WAL (so we can recover in case of crash)
        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(&buffer)?;
        }

        // 3) Insert into the correct shard
        {
            let mut shard_map_guard = self.sharded_tables.write().unwrap();
            let sharded_mem = shard_map_guard
                .get_mut(&self.name)
                .ok_or_else(|| OpNetError::new("No sharded memtable for collection"))?;

            sharded_mem.insert(key, buffer);

            // 4) If total memtable size across shards exceeds max, flush
            if sharded_mem.total_size() > sharded_mem.max_size() {
                let mut segmgr = self.segment_manager.lock().unwrap();
                // We'll call a function that knows how to flush a sharded memtable.
                // You can rename or adapt the logic inside your `SegmentManager`.
                segmgr.flush_sharded_memtable_to_segment(&self.name, sharded_mem, block_height)?;
                // Clear after flush
                sharded_mem.clear_all();
            }
        }

        Ok(())
    }

    /// Get a record by key.
    pub fn get(&self, key_args: &T::KeyArgs) -> OpNetResult<Option<T>> {
        let key = T::compose_key(key_args);

        // 1) Check the sharded memtable
        {
            let shard_map_guard = self.sharded_tables.read().unwrap();
            if let Some(sharded_mem) = shard_map_guard.get(&self.name) {
                if let Some(raw) = sharded_mem.get(&key) {
                    let mut reader = ByteReader::new(&raw[..]);
                    let obj = T::deserialize(&mut reader)?;
                    return Ok(Some(obj));
                }
            }
        }

        // 2) If not in memtable, check segment files
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::blockchain::reorg::ReorgManager;
    use crate::domain::db::segments::segment::SegmentManager;
    use crate::domain::db::wal::WAL;
    use crate::domain::generic::errors::OpNetResult;
    use crate::domain::io::{ByteReader, ByteWriter, CustomSerialize};
    use crate::domain::thread::concurrency::ThreadPool;
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::{Arc, Mutex, RwLock};
    use tempfile::TempDir;

    /// A basic record type for testing
    #[derive(Clone, Debug)]
    struct MockRecord {
        pub id: u64,
        pub data: String,
    }

    impl KeyProvider for MockRecord {
        type KeyArgs = u64;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }
        fn compose_key(args: &Self::KeyArgs) -> Vec<u8> {
            args.to_le_bytes().to_vec()
        }
    }

    impl CustomSerialize for MockRecord {
        fn serialize<W: Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()> {
            writer.write_u64(self.id)?;
            writer.write_var_bytes(self.data.as_bytes())?;
            Ok(())
        }

        fn deserialize<R: std::io::Read>(reader: &mut ByteReader<R>) -> OpNetResult<Self> {
            let id = reader.read_u64()?;
            let data_bytes = reader.read_var_bytes()?;
            let data_str = String::from_utf8(data_bytes)
                .map_err(|_| OpNetError::new("Invalid UTF-8 in data_str"))?;
            Ok(MockRecord { id, data: data_str })
        }
    }

    /// Helper to set up a test environment:
    ///  - A `SegmentManager` in a temporary directory
    ///  - A `ShardedMemTable` in a HashMap for our `collection_name`
    ///  - A WAL file in that same directory
    ///  - A ReorgManager
    ///  - Then we build a `Collection<MockRecord>`
    fn setup_collection_env(
        collection_name: &str,
        shard_count: usize,
        max_shard_size: usize,
    ) -> (TempDir, Collection<MockRecord>) {
        let tmp_dir = TempDir::new().expect("Failed to create temp dir");

        // Build a thread pool for the segment manager
        let pool = Arc::new(Mutex::new(ThreadPool::new(2)));
        let seg_manager = SegmentManager::new(tmp_dir.path(), pool)
            .expect("Creating SegmentManager must succeed");
        let seg_manager_arc = Arc::new(Mutex::new(seg_manager));

        // Build a single ShardedMemTable for `collection_name`
        let sharded_mem = ShardedMemTable::new(shard_count, max_shard_size);

        // Put it in a HashMap keyed by the collection name
        let mut map = HashMap::new();
        map.insert(collection_name.to_string(), sharded_mem);
        let sharded_tables_arc = Arc::new(RwLock::new(map));

        // Create a WAL
        let wal_path = tmp_dir.path().join("test_wal.log");
        let wal = WAL::open(wal_path).expect("WAL open must succeed");
        let wal_arc = Arc::new(Mutex::new(wal));

        // Create a ReorgManager
        let reorg = ReorgManager::new();
        let reorg_arc = Arc::new(Mutex::new(reorg));

        // Finally, create our Collection
        let metadata = CollectionMetadata::new(collection_name);
        let collection = Collection::<MockRecord>::new(
            collection_name.to_string(),
            sharded_tables_arc,
            wal_arc,
            seg_manager_arc,
            reorg_arc,
            metadata,
        );

        (tmp_dir, collection)
    }

    #[test]
    fn test_collection_basic_insert_and_get() {
        let (_tmp_dir, collection) = setup_collection_env("mock", 4, 10_000);

        // Insert a record
        let rec = MockRecord {
            id: 123,
            data: "Hello, World!".to_string(),
        };
        collection.insert(rec.clone(), 10).unwrap();

        // Retrieve the record
        let got = collection.get(&123).unwrap();
        assert!(got.is_some());
        let got_rec = got.unwrap();
        assert_eq!(got_rec.id, 123);
        assert_eq!(got_rec.data, "Hello, World!");

        // Try a missing record
        let none_rec = collection.get(&9999).unwrap();
        assert!(none_rec.is_none());
    }

    #[test]
    fn test_sharded_memtable_flush_trigger_by_size() {
        // We choose a small total max_size to force an early flush
        let (_tmp_dir, collection) = setup_collection_env("mock_flush", 4, 200);

        let large_data = "x".repeat(300); // 300 bytes
        let rec1 = MockRecord {
            id: 1,
            data: large_data.clone(),
        };
        collection.insert(rec1.clone(), 1).unwrap();

        // That alone should exceed the 200-byte max_size => flush
        // Confirm that reading it back pulls from the segment:
        let got1 = collection.get(&1).unwrap();
        assert!(got1.is_some(), "Must be found after flush");
        assert_eq!(got1.unwrap().data, large_data);
    }

    #[test]
    fn test_collection_manual_flush() {
        // High max_size so it doesn't auto-flush
        let (_tmp_dir, collection) = setup_collection_env("manual", 2, 999_999);

        // Insert a few small records
        for i in 0..5 {
            let rec = MockRecord {
                id: i,
                data: format!("val-{}", i),
            };
            collection.insert(rec, 100).unwrap();
        }

        // Now we flush manually by accessing the sharded memtable
        {
            let mut guard = collection.sharded_tables.write().unwrap();
            let sharded_mem = guard
                .get_mut("manual")
                .expect("No sharded memtable found for 'manual'");

            let mut segmgr = collection.segment_manager.lock().unwrap();
            segmgr
                .flush_sharded_memtable_to_segment("manual", sharded_mem, 100)
                .unwrap();
            sharded_mem.clear_all();
        }

        // Now data is on disk, so let's confirm we can retrieve it
        for i in 0..5 {
            let got = collection.get(&i).unwrap();
            assert!(
                got.is_some(),
                "Record with id={} must be found after manual flush",
                i
            );
            assert_eq!(got.unwrap().data, format!("val-{}", i));
        }
    }

    #[test]
    fn test_collection_concurrent_inserts() {
        use std::thread;

        let (_tmp_dir, collection) = setup_collection_env("concurrent_col", 4, 10_000);
        let coll_arc = Arc::new(collection);

        let mut handles = vec![];
        for t_id in 0..5 {
            let coll_clone = Arc::clone(&coll_arc);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let rec = MockRecord {
                        id: (t_id * 1_000 + i) as u64,
                        data: format!("thread{}-{}", t_id, i),
                    };
                    coll_clone.insert(rec, 100).unwrap();
                }
            }));
        }

        // Join
        for h in handles {
            h.join().expect("Thread must not panic");
        }

        // Confirm a sample key
        let test_key = 2 * 1000 + 50; // thread 2, i=50
        let got = coll_arc.get(&(test_key as u64)).unwrap();
        assert!(got.is_some());
        let got_rec = got.unwrap();
        assert_eq!(got_rec.id, test_key as u64);
        assert_eq!(got_rec.data, "thread2-50");
    }
}

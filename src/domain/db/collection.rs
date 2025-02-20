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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::blockchain::reorg::ReorgManager;
    use crate::domain::db::memtable::MemTable;
    use crate::domain::db::segments::segment::SegmentManager;
    use crate::domain::db::traits::key_provider::KeyProvider;
    use crate::domain::db::wal::WAL;
    use crate::domain::generic::errors::OpNetError;
    use crate::domain::generic::errors::OpNetResult;
    use crate::domain::io::{ByteReader, ByteWriter};
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::{Arc, Mutex, RwLock};
    use tempfile::TempDir;

    /// 1) Define a minimal mock record that implements `KeyProvider`.
    /// We’ll store two fields, but you can store anything you like.
    #[derive(Clone, Debug)]
    struct MockRecord {
        pub id: u64,
        pub data: String,
    }

    /// The "key" for `MockRecord` is just its `id` as a little‐endian byte array.
    impl KeyProvider for MockRecord {
        type KeyArgs = u64; // the user provides just a `u64` when looking up

        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }

        fn compose_key(args: &Self::KeyArgs) -> Vec<u8> {
            args.to_le_bytes().to_vec()
        }
    }

    /// We also implement `CustomSerialize` so that `Collection<T>` can call `record.serialize(...)`.
    impl crate::domain::io::CustomSerialize for MockRecord {
        fn serialize<W: Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()> {
            writer.write_u64(self.id)?;
            // Write the length + the bytes of `data`
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

    /// A helper function to create a new environment:
    /// - A temporary directory for the `SegmentManager`.
    /// - A single memtable in a map for our collection.
    /// - A real or mock WAL.
    /// - A ReorgManager.
    /// - Then we construct the `Collection<MockRecord>`.
    fn setup_collection_env(
        collection_name: &str,
        memtable_size: usize,
    ) -> (TempDir, Collection<MockRecord>) {
        // 1) Temp directory for segments
        let tmp_dir = TempDir::new().expect("temp dir creation failed");

        // 2) Thread pool for the segment manager
        let tp = Arc::new(Mutex::new(
            crate::domain::thread::concurrency::ThreadPool::new(2),
        ));

        // 3) Create a SegmentManager
        let seg_manager = SegmentManager::new(tmp_dir.path(), tp)
            .expect("Creating SegmentManager in test env must succeed");
        let seg_manager_arc = Arc::new(Mutex::new(seg_manager));

        // 4) Create a MemTable map with one entry for `collection_name`
        let mut memtable = MemTable::new(memtable_size);
        let mut mem_map = HashMap::new();
        mem_map.insert(collection_name.to_string(), memtable);

        let mem_map_arc = Arc::new(RwLock::new(mem_map));

        // 5) Create a mock or real WAL. For demo we create an actual WAL file in the tmp directory:
        let wal_path = tmp_dir.path().join("test_wal.log");
        let wal = WAL::open(wal_path).expect("WAL open must succeed");
        let wal_arc = Arc::new(Mutex::new(wal));

        // 6) Create a ReorgManager
        let reorg = ReorgManager::new();
        let reorg_arc = Arc::new(Mutex::new(reorg));

        // 7) Create the `CollectionMetadata` and then the actual `Collection<MockRecord>`
        let metadata = CollectionMetadata::new(collection_name);
        let collection = Collection::<MockRecord>::new(
            collection_name.to_string(),
            mem_map_arc,
            wal_arc,
            seg_manager_arc,
            reorg_arc,
            metadata,
        );
        (tmp_dir, collection)
    }

    #[test]
    fn test_collection_basic_insert_and_get() {
        let (_tmp_dir, collection) = setup_collection_env("mock", 10_000);

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
    fn test_collection_flush_trigger_by_size() {
        // We'll create a small memtable so that after a couple of inserts, it exceeds the max_size
        // and triggers a flush automatically.

        let memtable_size = 100; // something small
        let (_tmp_dir, collection) = setup_collection_env("mock_flush", memtable_size);

        // We create a record that is large enough to fill up the memtable quickly
        let large_data = "x".repeat(200); // 200 bytes
        let rec1 = MockRecord {
            id: 1,
            data: large_data.clone(),
        };
        collection.insert(rec1.clone(), 1).unwrap();

        // Because each record is over 200 bytes once serialized, this second insert
        // should exceed the memtable's max_size and trigger a flush.
        let rec2 = MockRecord {
            id: 2,
            data: large_data.clone(),
        };
        collection.insert(rec2.clone(), 2).unwrap();

        // Now the memtable is presumably flushed and cleared. Let's confirm we can read from disk:
        let got1 = collection.get(&1).unwrap();
        assert!(got1.is_some(), "Record #1 must be found after flush");
        assert_eq!(got1.unwrap().data, large_data);

        let got2 = collection.get(&2).unwrap();
        assert!(got2.is_some(), "Record #2 must also be found after flush");
        assert_eq!(got2.unwrap().data, large_data);
    }

    #[test]
    fn test_collection_manual_flush() {
        // We'll do repeated inserts without hitting max_size. Then manually call flush on the memtable
        // so that the data goes to the segment.

        let (_tmp_dir, collection) = setup_collection_env("manual", 999_999);

        // Insert a few small records
        for i in 0..5 {
            let rec = MockRecord {
                id: i,
                data: format!("val-{}", i),
            };
            collection.insert(rec, 100).unwrap();
        }

        // Data is all in memtable so far, let's flush it ourselves
        {
            // We'll need to get the memtable from the map and flush it using segment_manager
            let segmgr_arc = collection.segment_manager.clone();
            let mut segmgr = segmgr_arc.lock().unwrap();

            // Acquire the memtable for "manual"
            let mut guard = collection.memtables.write().unwrap();
            let mem = guard
                .get_mut("manual")
                .expect("memtable for 'manual' must exist");

            segmgr
                .flush_memtable_to_segment("manual", mem, 100)
                .unwrap();
            mem.clear(); // if you want it cleared after flush
        }

        // Now data should be on disk, let's confirm we can retrieve it
        for i in 0..5 {
            let got = collection.get(&i).unwrap();
            assert!(got.is_some(), "Record must be found after manual flush");
            assert_eq!(got.unwrap().data, format!("val-{}", i));
        }
    }

    #[test]
    fn test_collection_wal_appends() {
        // We'll just verify that calling `insert()` actually appends to the WAL.
        // Then we can read the WAL's content or check the file size.

        let (tmp_dir, collection) = setup_collection_env("waltest", 10_000);

        // Insert some records
        let rec = MockRecord {
            id: 1,
            data: "abc".into(),
        };
        collection.insert(rec, 10).unwrap();

        let rec2 = MockRecord {
            id: 2,
            data: "def".into(),
        };
        collection.insert(rec2, 10).unwrap();

        // Now let's check the WAL file size
        // The WAL path is known from setup_collection_env
        let wal_path = tmp_dir.path().join("test_wal.log");
        let metadata = std::fs::metadata(&wal_path).expect("WAL metadata read must succeed");
        assert!(metadata.len() > 0, "WAL should not be empty after inserts");
    }

    #[test]
    fn test_collection_concurrent_inserts() {
        // We'll test concurrency by spawning multiple threads that insert different records
        // into the same collection. The RwLock around memtables + the concurrency logic in SegmentManager
        // should ensure no corruption.

        use std::thread;

        let (_tmp_dir, collection) = setup_collection_env("concurrent_col", 10_000);
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

        // Now let's check if we can retrieve a few random keys
        let coll_final = coll_arc;
        // e.g. from thread #2, key= (2*1000 + 50) = 2050
        let test_key = 2 * 1000 + 50;
        let got = coll_final.get(&(test_key as u64)).unwrap();
        assert!(got.is_some());
        let got_rec = got.unwrap();
        assert_eq!(got_rec.id, 2050);
        assert_eq!(got_rec.data, "thread2-50");
    }
}

use crate::domain::db::collection::Collection;
use crate::domain::db::collections::utxo::Utxo;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter, CustomSerialize};
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct UtxoByAddressRef {
    pub address: [u8; 33],
    pub tx_id: [u8; 32],
    pub output_index: u32,
}

impl CustomSerialize for UtxoByAddressRef {
    fn serialize<W: Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()> {
        // Write 20-byte address
        writer.write_bytes(&self.address)?;
        // Write 32-byte txid
        writer.write_bytes(&self.tx_id)?;
        // Write the 4-byte output index
        writer.write_u32(self.output_index)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut ByteReader<R>) -> OpNetResult<Self> {
        // Read address
        let addr_bytes = reader.read_bytes(33)?;
        let mut address = [0u8; 33];
        address.copy_from_slice(&addr_bytes);

        // Read txid
        let txid_bytes = reader.read_bytes(32)?;
        let mut tx_id = [0u8; 32];
        tx_id.copy_from_slice(&txid_bytes);

        // Read vout
        let vout = reader.read_u32()?;

        Ok(UtxoByAddressRef {
            address,
            tx_id,
            output_index: vout,
        })
    }
}

impl KeyProvider for UtxoByAddressRef {
    // The "lookup arguments" if someone wants to fetch it exactly would be (address, txid, output_index).
    type KeyArgs = ([u8; 33], [u8; 32], u32);

    fn primary_key(&self) -> Vec<u8> {
        // Construct [ address (20 bytes) + txid (32 bytes) + outputIndex (4 bytes) ]
        let mut buf = Vec::with_capacity(33 + 32 + 4);
        buf.extend_from_slice(&self.address);
        buf.extend_from_slice(&self.tx_id);
        buf.extend_from_slice(&self.output_index.to_le_bytes());
        buf
    }

    fn compose_key(args: &Self::KeyArgs) -> Vec<u8> {
        let (addr, txid, vout) = args;
        let mut buf = Vec::with_capacity(33 + 32 + 4);
        buf.extend_from_slice(addr);
        buf.extend_from_slice(txid);
        buf.extend_from_slice(&vout.to_le_bytes());
        buf
    }
}

impl Collection<UtxoByAddressRef> {
    /// Finds all references for a given address, then fetches full UTXOs
    /// from the main collection. Applies `deleted_at_block == None` +
    /// optional `min_value` filter, respecting `limit`.
    pub fn find_unspent_utxos_for_address(
        &self,
        address: [u8; 33],
        min_value: Option<u64>,
        limit: usize,
        main_utxo_collection: &Collection<Utxo>,
    ) -> OpNetResult<Vec<Utxo>> {
        use crate::domain::io::ByteReader;
        use std::collections::HashSet;

        // ------------------------------------------------------
        // 1) Build start/end keys for segment range lookup
        // ------------------------------------------------------
        let mut start_key = Vec::with_capacity(36 + 33);
        start_key.extend_from_slice(&address);
        start_key.extend_from_slice(&[0u8; 36]);

        let mut end_key = Vec::with_capacity(36 + 33);
        end_key.extend_from_slice(&address);
        end_key.extend_from_slice(&[0xFF; 36]);

        // ------------------------------------------------------
        // 2) Collect from the *sharded* memtable
        // ------------------------------------------------------
        let mut memtable_refs = Vec::new();
        {
            // Lock the RwLock that holds: HashMap<String, ShardedMemTable>
            let shard_map_guard = self.sharded_tables.read().unwrap();
            // Get the ShardedMemTable for this collection name
            let sharded_mem = shard_map_guard
                .get(&self.name)
                .ok_or_else(|| OpNetError::new("No sharded memtable for this collection"))?;

            // For each shard, lock and iterate over key-value pairs
            for shard_mutex in &sharded_mem.shards {
                let shard = shard_mutex.lock().unwrap();
                // Scan the entire shard HashMap for keys matching the first 20 bytes == address
                for (k, v) in &shard.data {
                    if k.len() >= 33 && &k[0..33] == address {
                        let mut rdr = ByteReader::new(&v[..]);
                        if let Ok(ref_obj) = UtxoByAddressRef::deserialize(&mut rdr) {
                            memtable_refs.push(ref_obj);
                        }
                    }
                }
            }
        } // sharded_tables lock is DROPPED here

        // ------------------------------------------------------
        // 3) Collect from SEGMENTS
        // ------------------------------------------------------
        let mut segment_refs = Vec::new();
        {
            let segmgr = self.segment_manager.lock().unwrap();
            let raw_refs = segmgr.find_values_in_range(&self.name, &start_key, &end_key, limit)?;

            for raw in raw_refs {
                let mut rdr = ByteReader::new(&raw[..]);
                if let Ok(ref_obj) = UtxoByAddressRef::deserialize(&mut rdr) {
                    segment_refs.push(ref_obj);
                }
            }
        }

        // ------------------------------------------------------
        // 4) Merge memtable + segment references
        // ------------------------------------------------------
        // We put memtable refs first (assuming they're "newer"), then segment refs.
        // Then we deduplicate by (tx_id, vout).
        let mut all_refs = memtable_refs;
        all_refs.extend(segment_refs);

        let mut final_refs = Vec::new();
        let mut seen_pairs = HashSet::new();

        for r in all_refs {
            let pair = (r.tx_id, r.output_index);
            if !seen_pairs.contains(&pair) {
                final_refs.push(r);
                seen_pairs.insert(pair);
                if final_refs.len() >= limit {
                    break;
                }
            }
        }

        // ------------------------------------------------------
        // 5) For each reference, call main_utxo_collection.get
        // ------------------------------------------------------
        let mut results = Vec::new();
        let min_val = min_value.unwrap_or(0);

        for r in final_refs {
            if let Some(utxo) = main_utxo_collection.get(&(r.tx_id, r.output_index))? {
                if utxo.deleted_at_block.is_none() && utxo.amount >= min_val {
                    results.push(utxo);
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests_utxo_by_address {
    use super::*;
    use crate::domain::db::__test__::helpper::{make_test_db, teardown_fs};
    use crate::domain::generic::errors::OpNetResult;
    use std::collections::HashSet;

    /// Helper for quickly building a main Utxo
    fn make_utxo(txid_byte: u8, vout: u32, address: [u8; 33], amount: u64) -> Utxo {
        let mut tx_id = [0u8; 32];
        tx_id[0] = txid_byte;
        Utxo {
            tx_id,
            output_index: vout,
            address,
            amount,
            script_pubkey: vec![0xAB],
            deleted_at_block: None,
        }
    }

    /// Helper for building the address-reference collection entry
    fn make_address_ref(address: [u8; 33], tx_id: [u8; 32], vout: u32) -> UtxoByAddressRef {
        UtxoByAddressRef {
            address,
            tx_id,
            output_index: vout,
        }
    }

    #[test]
    fn test_find_unspent_utxos_for_address_no_results() -> OpNetResult<()> {
        let test_name = "find_unspent_none";
        let db = make_test_db(test_name, 0)?;
        // Register main UTXO + address-ref collections
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        // We do not insert anything, so no results for any address
        let empty_addr = [0x00; 33];
        let found = addr_coll.find_unspent_utxos_for_address(empty_addr, None, 10, &main_coll)?;
        assert!(found.is_empty(), "Should find no UTXOs for empty DB");

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_find_unspent_utxos_for_address_basic() -> OpNetResult<()> {
        let test_name = "find_unspent_basic";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        // Insert a couple of UTXOs
        let addr1 = [0xAA; 33];
        let utxo1 = make_utxo(0x01, 0, addr1, 1000);
        main_coll.insert(utxo1.clone(), 10)?;

        // Insert address-ref
        let addr_ref1 = make_address_ref(addr1, utxo1.tx_id, utxo1.output_index);
        addr_coll.insert(addr_ref1, 10)?;

        let addr2 = [0xBB; 33];
        let utxo2 = make_utxo(0x02, 0, addr2, 5000);
        main_coll.insert(utxo2.clone(), 11)?;

        // Insert address-ref
        let addr_ref2 = make_address_ref(addr2, utxo2.tx_id, utxo2.output_index);
        addr_coll.insert(addr_ref2, 11)?;

        // Now find for addr1 => should get utxo1
        let found_addr1 = addr_coll.find_unspent_utxos_for_address(addr1, None, 10, &main_coll)?;
        assert_eq!(found_addr1.len(), 1);
        assert_eq!(found_addr1[0].amount, 1000);

        // Find for addr2 => should get utxo2
        let found_addr2 = addr_coll.find_unspent_utxos_for_address(addr2, None, 10, &main_coll)?;
        assert_eq!(found_addr2.len(), 1);
        assert_eq!(found_addr2[0].amount, 5000);

        // A random address => no results
        let rand_addr = [0xCC; 33];
        let found_rand =
            addr_coll.find_unspent_utxos_for_address(rand_addr, None, 10, &main_coll)?;
        assert!(found_rand.is_empty());

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_find_unspent_utxos_for_address_spent_and_minvalue() -> OpNetResult<()> {
        let test_name = "find_unspent_spent";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        let addr = [0x99; 33];

        // Insert 3 UTXOs to the same address, different amounts
        let utxo_a = make_utxo(0xA0, 0, addr, 1000);
        let utxo_b = make_utxo(0xB0, 1, addr, 3000);
        let utxo_c = make_utxo(0xC0, 2, addr, 10_000);

        main_coll.insert(utxo_a.clone(), 100)?;
        main_coll.insert(utxo_b.clone(), 101)?;
        main_coll.insert(utxo_c.clone(), 102)?;

        addr_coll.insert(
            make_address_ref(addr, utxo_a.tx_id, utxo_a.output_index),
            100,
        )?;
        addr_coll.insert(
            make_address_ref(addr, utxo_b.tx_id, utxo_b.output_index),
            101,
        )?;
        addr_coll.insert(
            make_address_ref(addr, utxo_c.tx_id, utxo_c.output_index),
            102,
        )?;

        // Spend one of them (utxo_b) at block 103
        main_coll.mark_spent(&(utxo_b.tx_id, utxo_b.output_index), 103)?;

        // Query no min_value => expect utxo_a + utxo_c, skip b because it's spent
        let found_1 = addr_coll.find_unspent_utxos_for_address(addr, None, 10, &main_coll)?;
        let set_1: HashSet<u64> = found_1.iter().map(|u| u.amount).collect();
        assert_eq!(set_1.len(), 2);
        assert!(set_1.contains(&1000));
        assert!(set_1.contains(&10_000));

        // Query with min_value=2000 => only utxo_c qualifies
        let found_2 = addr_coll.find_unspent_utxos_for_address(addr, Some(2000), 10, &main_coll)?;
        assert_eq!(found_2.len(), 1);
        assert_eq!(found_2[0].amount, 10_000);

        // Mark c as spent too, verify only a remains
        main_coll.mark_spent(&(utxo_c.tx_id, utxo_c.output_index), 104)?;
        let found_3 = addr_coll.find_unspent_utxos_for_address(addr, None, 10, &main_coll)?;
        assert_eq!(found_3.len(), 1);
        assert_eq!(found_3[0].amount, 1000);

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_find_unspent_utxos_for_address_limit() -> OpNetResult<()> {
        let test_name = "find_unspent_limit";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        let addr = [0x55; 33];

        // Insert 5 UTXOs
        for i in 0..5 {
            let utxo = make_utxo(0xAA, i, addr, 100 + i as u64);
            main_coll.insert(utxo.clone(), 200 + i as u64)?;
            addr_coll.insert(
                make_address_ref(addr, utxo.tx_id, utxo.output_index),
                200 + i as u64,
            )?;
        }

        // Query limit=2 => should only fetch 2 unspent
        let found = addr_coll.find_unspent_utxos_for_address(addr, None, 2, &main_coll)?;
        assert_eq!(found.len(), 2, "Should only return 2 results due to limit");

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_find_unspent_utxos_for_address_memtable_versus_segment() -> OpNetResult<()> {
        // This test ensures we get coverage of the memtable scanning logic
        // (when references haven't been flushed yet), plus the segment logic.
        let test_name = "find_memtable_vs_segment";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        let addr = [0x77; 33];

        // 1) Insert a UTXO => flush to segment
        let utxo1 = make_utxo(0x11, 0, addr, 500);
        main_coll.insert(utxo1.clone(), 10)?;
        addr_coll.insert(make_address_ref(addr, utxo1.tx_id, utxo1.output_index), 10)?;

        // Force flush (so that entry is in a segment)
        db.flush_all(10)?;

        // 2) Insert a second UTXO but do *not* flush => it remains in memtable
        let utxo2 = make_utxo(0x22, 1, addr, 999);
        main_coll.insert(utxo2.clone(), 11)?;
        addr_coll.insert(make_address_ref(addr, utxo2.tx_id, utxo2.output_index), 11)?;

        // 3) Query => Should find both (1 in segment, 1 in memtable)
        let found = addr_coll.find_unspent_utxos_for_address(addr, None, 10, &main_coll)?;
        let amounts: HashSet<_> = found.iter().map(|u| u.amount).collect();
        assert_eq!(amounts.len(), 2);
        assert!(amounts.contains(&500));
        assert!(amounts.contains(&999));

        teardown_fs(test_name);

        Ok(())
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use crate::domain::db::__test__::helpper::{make_test_db, teardown_fs};
    use crate::domain::generic::errors::OpNetResult;
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    //#[ignore]
    fn test_insert_1_million_utxos_random_addresses() -> OpNetResult<()> {
        let test_name = "perf_1m_utxos_random";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        let total_utxos = 1_000_000;
        let block_size = 100_000; // e.g. 100 blocks
        let mut current_block = 100;

        // We'll pick a single "known" address that we'll use occasionally, so we can reliably fetch 1000 from it later.
        let known_address = [0xAA; 33];

        let start_time = Instant::now();
        let mut total_inserted = 0usize;

        // We'll insert in chunks
        for chunk_start in (0..total_utxos).step_by(block_size) {
            let block_start_time = Instant::now();

            for i in chunk_start..(chunk_start + block_size) {
                // Decide which address to use:
                //
                // so we definitely have enough for a final 1000 fetch.
                let address_chance = (i % 1000) == 0; // 1 out of 50 =>
                let address: [u8; 33] = if address_chance {
                    known_address
                } else {
                    let mut arr = [0u8; 33];
                    arr[0] = (i & 255) as u8;
                    arr[1] = ((i >> 8) & 255) as u8;
                    arr[2] = ((i >> 16) & 255) as u8;
                    arr[3] = ((i >> 24) & 255) as u8;
                    arr
                };

                // Build a unique tx_id
                let mut txid = [0u8; 32];
                txid[0] = (i & 0xFF) as u8;
                txid[1] = ((i >> 8) & 0xFF) as u8;
                txid[2] = ((i >> 16) & 0xFF) as u8;
                txid[3] = ((i >> 24) & 0xFF) as u8;

                // Insert
                let utxo = Utxo {
                    tx_id: txid,
                    output_index: i as u32,
                    address,
                    amount: 1_000 + i as u64,
                    script_pubkey: vec![0xAB, 0xCD],
                    deleted_at_block: None,
                };

                main_coll.insert(utxo.clone(), current_block)?;
                addr_coll.insert(
                    UtxoByAddressRef {
                        address,
                        tx_id: txid,
                        output_index: i as u32,
                    },
                    current_block,
                )?;
            }

            db.flush_all(current_block)?;

            total_inserted += block_size;
            current_block += 1;

            let block_duration = block_start_time.elapsed();
            println!(
                "Inserted block of {} UTXOs in {:?} (total so far: {}).",
                block_size, block_duration, total_inserted
            );
        }

        let total_duration = start_time.elapsed();
        println!(
            "Inserted {} UTXOs (with random addresses) in {:?}. Average: {:?}/UTXO",
            total_utxos,
            total_duration,
            total_duration / (total_utxos as u32),
        );

        // Now let's fetch up to 1000 from the "known" address:
        let fetch_start = Instant::now();
        let found =
            addr_coll.find_unspent_utxos_for_address(known_address, None, 10000, &main_coll)?;
        let fetch_time = fetch_start.elapsed();
        println!(
            "Fetched up to 1000 unspent from known address in {:?}. Found {} total.",
            fetch_time,
            found.len()
        );
        assert!(
            found.len() >= 1000,
            "We expect to find at least 1000 in the known address"
        );

        //teardown_fs(test_name);
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_insert_1_million_in_parallel() -> OpNetResult<()> {
        let test_name = "perf_1m_parallel";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        db.register_collection("utxo_by_address")?;

        let main_coll = Arc::new(db.collection::<Utxo>("utxo")?);
        let addr_coll = Arc::new(db.collection::<UtxoByAddressRef>("utxo_by_address")?);

        let total_utxos = 1_000_000;
        let num_threads = 4;
        let utxos_per_thread = total_utxos / num_threads;

        // Start timing
        let start_time = std::time::Instant::now();

        let mut handles = Vec::new();

        for t_id in 0..num_threads {
            let main_clone = Arc::clone(&main_coll);
            let addr_clone = Arc::clone(&addr_coll);

            let handle = std::thread::spawn(move || -> OpNetResult<()> {
                let start_i = t_id * utxos_per_thread;
                let end_i = start_i + utxos_per_thread;

                // We'll just use a single block for each thread in this example
                let block_height = 1000 + t_id as u64;

                for i in start_i..end_i {
                    let mut txid = [0u8; 32];
                    txid[0] = (i & 0xFF) as u8;
                    txid[1] = ((i >> 8) & 0xFF) as u8;
                    txid[2] = ((i >> 16) & 0xFF) as u8;
                    txid[3] = ((i >> 24) & 0xFF) as u8;

                    // Use a random or static address
                    let address = [t_id as u8; 33]; // each thread uses different address
                    let utxo = Utxo {
                        tx_id: txid,
                        output_index: i as u32,
                        address,
                        amount: 9999,
                        script_pubkey: vec![0xAB, 0xCD],
                        deleted_at_block: None,
                    };

                    main_clone.insert(utxo.clone(), block_height)?;
                    let addr_ref = UtxoByAddressRef {
                        address,
                        tx_id: utxo.tx_id,
                        output_index: utxo.output_index,
                    };
                    addr_clone.insert(addr_ref, block_height)?;
                }

                // Done
                Ok(())
            });
            handles.push(handle);
        }

        // Join all threads
        for h in handles {
            h.join().unwrap()?;
        }

        // Then flush everything once at the end, if desired
        db.flush_all(2000)?;

        let elapsed = start_time.elapsed();
        println!(
            "Inserted {} UTXOs in parallel with {} threads in {:?}",
            total_utxos, num_threads, elapsed
        );

        teardown_fs(test_name);
        Ok(())
    }
}

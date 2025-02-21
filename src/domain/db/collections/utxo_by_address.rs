use crate::domain::db::collection::Collection;
use crate::domain::db::collections::utxo::Utxo;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::generic::errors::{OpNetError, OpNetResult};
use crate::domain::io::{ByteReader, ByteWriter, CustomSerialize};
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct UtxoByAddressRef {
    pub address: [u8; 20],
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
        let addr_bytes = reader.read_bytes(20)?;
        let mut address = [0u8; 20];
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
    type KeyArgs = ([u8; 20], [u8; 32], u32);

    fn primary_key(&self) -> Vec<u8> {
        // Construct [ address (20 bytes) + txid (32 bytes) + outputIndex (4 bytes) ]
        let mut buf = Vec::with_capacity(20 + 32 + 4);
        buf.extend_from_slice(&self.address);
        buf.extend_from_slice(&self.tx_id);
        buf.extend_from_slice(&self.output_index.to_le_bytes());
        buf
    }

    fn compose_key(args: &Self::KeyArgs) -> Vec<u8> {
        let (addr, txid, vout) = args;
        let mut buf = Vec::with_capacity(20 + 32 + 4);
        buf.extend_from_slice(addr);
        buf.extend_from_slice(txid);
        buf.extend_from_slice(&vout.to_le_bytes());
        buf
    }
}

impl Collection<UtxoByAddressRef> {
    /// Finds all references for a given address, then fetches full UTXOs from the main collection.
    /// Applies `deleted_at_block == None` + optional `min_value` filter.
    pub fn find_unspent_utxos_for_address(
        &self,
        address: [u8; 20],
        min_value: Option<u64>,
        limit: usize,
        main_utxo_collection: &Collection<Utxo>,
    ) -> OpNetResult<Vec<Utxo>> {
        use crate::domain::io::ByteReader;
        use std::collections::HashSet;

        // Build start_key = [address + all-zeros], end_key = [address + all-FF]
        let mut start_key = Vec::with_capacity(56);
        start_key.extend_from_slice(&address);
        start_key.extend_from_slice(&[0u8; 36]);

        let mut end_key = Vec::with_capacity(56);
        end_key.extend_from_slice(&address);
        end_key.extend_from_slice(&[0xFF; 36]);

        // 1) Query the address-based index in the segment files
        let segmgr = self.segment_manager.lock().unwrap();
        let raw_refs = segmgr.find_values_in_range(&self.name, &start_key, &end_key, limit)?;

        // 2) Query the memtable for any references that haven't been flushed
        //    (Naive approach, scanning the entire memtable HashMap.)
        let mem_guard = self.memtables.read().unwrap();
        let mem = mem_guard
            .get(&self.name)
            .ok_or_else(|| OpNetError::new("No memtable for this collection"))?;

        let mut memtable_refs = Vec::new();
        for (k, v) in &mem.data {
            // Check if the first 20 bytes match the address
            if k.len() >= 20 && &k[0..20] == address {
                let mut rdr = ByteReader::new(&v[..]);
                if let Ok(ref_obj) = UtxoByAddressRef::deserialize(&mut rdr) {
                    memtable_refs.push(ref_obj);
                }
            }
        }
        drop(mem_guard); // done reading memtable

        // Convert segment results into UtxoByAddressRef
        let mut all_refs = Vec::new();
        for raw in raw_refs {
            let mut rdr = ByteReader::new(&raw[..]);
            if let Ok(ref_obj) = UtxoByAddressRef::deserialize(&mut rdr) {
                all_refs.push(ref_obj);
            }
        }

        // Insert memtable references “on top” (assuming they’re newer)
        for r in memtable_refs {
            all_refs.insert(0, r);
        }

        // 3) De‐duplicate references by (tx_id, vout) in case older segments also contain them
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

        // 4) For each reference, do a second .get(...) in the main collection
        let mut results = Vec::new();
        let min_val = min_value.unwrap_or(0);
        for r in final_refs {
            // The main UTXO key is (tx_id, output_index)
            let maybe_utxo = main_utxo_collection.get(&(r.tx_id, r.output_index))?;
            if let Some(utxo) = maybe_utxo {
                // If the record is unspent and meets min_value
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
    fn make_utxo(txid_byte: u8, vout: u32, address: [u8; 20], amount: u64) -> Utxo {
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
    fn make_address_ref(address: [u8; 20], tx_id: [u8; 32], vout: u32) -> UtxoByAddressRef {
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
        let empty_addr = [0x00; 20];
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
        let addr1 = [0xAA; 20];
        let utxo1 = make_utxo(0x01, 0, addr1, 1000);
        main_coll.insert(utxo1.clone(), 10)?;

        // Insert address-ref
        let addr_ref1 = make_address_ref(addr1, utxo1.tx_id, utxo1.output_index);
        addr_coll.insert(addr_ref1, 10)?;

        let addr2 = [0xBB; 20];
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
        let rand_addr = [0xCC; 20];
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

        let addr = [0x99; 20];

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

        let addr = [0x55; 20];

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

        println!("making elements.");

        let main_coll = db.collection::<Utxo>("utxo")?;
        let addr_coll = db.collection::<UtxoByAddressRef>("utxo_by_address")?;

        let addr = [0x77; 20];

        // 1) Insert a UTXO => flush to segment
        let utxo1 = make_utxo(0x11, 0, addr, 500);
        main_coll.insert(utxo1.clone(), 10)?;
        addr_coll.insert(make_address_ref(addr, utxo1.tx_id, utxo1.output_index), 10)?;

        println!("making elements 2.");

        // Force flush (so that entry is in a segment)
        db.flush_all(10)?;

        println!("fushed elements.");

        // 2) Insert a second UTXO but do *not* flush => it remains in memtable
        let utxo2 = make_utxo(0x22, 1, addr, 999);
        main_coll.insert(utxo2.clone(), 11)?;
        addr_coll.insert(make_address_ref(addr, utxo2.tx_id, utxo2.output_index), 11)?;

        println!("inserted elements.");

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

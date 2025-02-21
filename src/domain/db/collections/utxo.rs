use crate::domain::db::collection::Collection;
use crate::domain::db::traits::key_provider::KeyProvider;
use crate::domain::generic::errors::OpNetResult;
use crate::domain::io::{ByteReader, ByteWriter, CustomSerialize};

#[derive(Clone, Debug)]
pub struct Utxo {
    pub tx_id: [u8; 32],
    pub output_index: u32,
    pub address: [u8; 20],
    pub amount: u64,
    pub script_pubkey: Vec<u8>,
    pub deleted_at_block: Option<u64>,
}

impl CustomSerialize for Utxo {
    fn serialize<W: std::io::Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()> {
        writer.write_bytes(&self.tx_id)?;
        writer.write_u32(self.output_index)?;
        writer.write_bytes(&self.address)?;
        writer.write_u64(self.amount)?;
        writer.write_var_bytes(&self.script_pubkey)?;

        // Serialize deleted_at_block as a bool + block number if present
        match self.deleted_at_block {
            Some(h) => {
                writer.write_bool(true)?;
                writer.write_u64(h)?;
            }
            None => {
                writer.write_bool(false)?;
            }
        }

        Ok(())
    }

    fn deserialize<R: std::io::Read>(reader: &mut ByteReader<R>) -> OpNetResult<Self> {
        let txid_bytes = reader.read_bytes(32)?;
        let mut tx_id = [0u8; 32];
        tx_id.copy_from_slice(&txid_bytes);

        let output_index = reader.read_u32()?;
        let addr_bytes = reader.read_bytes(20)?;
        let mut address = [0u8; 20];
        address.copy_from_slice(&addr_bytes);

        let amount = reader.read_u64()?;
        let script_pubkey = reader.read_var_bytes()?;

        let has_deleted = reader.read_bool()?;
        let deleted_at_block = if has_deleted {
            Some(reader.read_u64()?)
        } else {
            None
        };

        Ok(Utxo {
            tx_id,
            output_index,
            address,
            amount,
            script_pubkey,
            deleted_at_block,
        })
    }
}

// The “primary” key for direct lookups by (txid, output_index).
// We keep this as your original “Utxo” type’s KeyProvider:
impl KeyProvider for Utxo {
    type KeyArgs = ([u8; 32], u32);

    fn primary_key(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(&self.tx_id);
        buf.extend_from_slice(&self.output_index.to_le_bytes());
        buf
    }

    fn compose_key(args: &Self::KeyArgs) -> Vec<u8> {
        let (txid, vout) = args;
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(txid);
        buf.extend_from_slice(&vout.to_le_bytes());
        buf
    }
}

impl Collection<Utxo> {
    /// Mark a UTXO as spent by setting `deleted_at_block = Some(spent_block)`.
    pub fn mark_spent(
        &self,
        key_args: &(<Utxo as KeyProvider>::KeyArgs),
        spent_block: u64,
    ) -> OpNetResult<()> {
        if let Some(mut utxo) = self.get(key_args)? {
            utxo.deleted_at_block = Some(spent_block);
            self.insert(utxo, spent_block)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests_mark_spent {
    use super::*;
    use crate::domain::db::__test__::helpper::{make_test_db, teardown_fs};
    use crate::domain::generic::errors::OpNetResult;

    /// Helper: quickly create a Utxo with the given txid, output_index, etc.
    fn make_utxo(txid_byte: u8, vout: u32, amount: u64) -> Utxo {
        let mut tx_id = [0u8; 32];
        tx_id[0] = txid_byte;
        Utxo {
            tx_id,
            output_index: vout,
            address: [0xAA; 20],
            amount,
            script_pubkey: vec![0xAB, 0xCD],
            deleted_at_block: None,
        }
    }

    #[test]
    fn test_mark_spent_nonexistent_key() -> OpNetResult<()> {
        let test_name = "mark_spent_nonexistent";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        let coll = db.collection::<Utxo>("utxo")?;

        // Attempt mark_spent on a nonexistent (txid, vout)
        let nonexistent_key = ([0xFF; 32], 999);
        coll.mark_spent(&nonexistent_key, 1000)?;
        // Should succeed with no error, but do nothing.

        // Confirm it's still not found
        let item = coll.get(&nonexistent_key)?;
        assert!(item.is_none(), "No record should exist for that key.");

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_mark_spent_existing() -> OpNetResult<()> {
        let test_name = "mark_spent_existing";
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        let coll = db.collection::<Utxo>("utxo")?;

        // Insert a new UTXO
        let utxo = make_utxo(0xAB, 1, 5000);
        coll.insert(utxo.clone(), 100)?;

        // Check it is unspent at first
        let key = (utxo.tx_id, utxo.output_index);
        let fetched_before = coll.get(&key)?;
        assert!(fetched_before.is_some());
        assert_eq!(fetched_before.as_ref().unwrap().deleted_at_block, None);

        // Mark spent
        coll.mark_spent(&key, 101)?;

        // Now fetch again => should have deleted_at_block = Some(101)
        let fetched_after = coll.get(&key)?;
        assert!(fetched_after.is_some());
        assert_eq!(fetched_after.as_ref().unwrap().deleted_at_block, Some(101));

        teardown_fs(test_name);

        Ok(())
    }

    #[test]
    fn test_mark_spent_idempotent() -> OpNetResult<()> {
        let test_name = "mark_spent_idempotent";
        // We show that calling mark_spent multiple times overwrites the block each time
        let db = make_test_db(test_name, 0)?;
        db.register_collection("utxo")?;
        let coll = db.collection::<Utxo>("utxo")?;

        let utxo = make_utxo(0x10, 2, 9999);
        coll.insert(utxo.clone(), 200)?;

        // Spend at block 201
        let key = (utxo.tx_id, utxo.output_index);
        coll.mark_spent(&key, 201)?;
        let after_201 = coll.get(&key)?.unwrap();
        assert_eq!(after_201.deleted_at_block, Some(201));

        // Spend again at block 202 => it updates the spent block
        coll.mark_spent(&key, 202)?;
        let after_202 = coll.get(&key)?.unwrap();
        assert_eq!(after_202.deleted_at_block, Some(202));

        teardown_fs(test_name);

        Ok(())
    }
}

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
}

impl CustomSerialize for Utxo {
    fn serialize<W: std::io::Write>(&self, writer: &mut ByteWriter<W>) -> OpNetResult<()> {
        writer.write_bytes(&self.tx_id)?;
        writer.write_u32(self.output_index)?;
        writer.write_bytes(&self.address)?;
        writer.write_u64(self.amount)?;
        writer.write_var_bytes(&self.script_pubkey)?;
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

        Ok(Utxo {
            tx_id,
            output_index,
            address,
            amount,
            script_pubkey,
        })
    }
}

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

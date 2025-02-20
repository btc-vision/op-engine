use crate::domain::io::CustomSerialize;

/// The trait that all stored record types must implement.
/// - `KeyArgs` is the type you use to *look up* a record. (For UTXOs, it's (`[u8; 32]`, `u32`))
/// - `compose_key(args: &Self::KeyArgs) -> Vec<u8>` is a static method that
///    takes the "search arguments" and returns the key bytes.
/// - `primary_key(&self) -> Vec<u8>` builds the same key from the record's own fields.
pub trait KeyProvider: CustomSerialize + Clone + Send + Sync + 'static {
    type KeyArgs: Send + Sync;
    fn primary_key(&self) -> Vec<u8>;
    fn compose_key(args: &Self::KeyArgs) -> Vec<u8>;
}

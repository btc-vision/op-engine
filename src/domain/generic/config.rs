#[derive(Clone, Debug)]
pub struct DbConfig {
    pub data_path: String,
    pub wal_path: String,
    pub num_threads: usize,
    pub memtable_size: usize,
    pub height: u64
}

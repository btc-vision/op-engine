use crate::domain::engine::OpNetDB;
use crate::domain::generic::config::DbConfig;
use crate::domain::generic::errors::OpNetResult;
use std::fs;

/// A helper to build a default test config for each test,
/// using a unique path to avoid collisions.
pub fn make_test_config(test_name: &str, start_height: u64) -> DbConfig {
    let data_path = format!("./test_data_{test_name}");
    let wal_path = format!("./test_data_{test_name}/wal.log");
    DbConfig {
        data_path,
        wal_path,
        num_threads: 8,
        memtable_size: 1024 * 1024 * 1024,
        height: start_height,
    }
}

pub fn make_test_db(test_name: &str, start_height: u64) -> OpNetResult<OpNetDB> {
    setup_fs(test_name)?;

    let config = make_test_config(test_name, start_height);
    let db = OpNetDB::new(config)?;
    Ok(db)
}

/// A helper that ensures we start fresh for each test.
/// Removes any existing directory, then creates it.
pub fn setup_fs(test_name: &str) -> OpNetResult<()> {
    let dir = format!("./test_data_{test_name}");
    let _ = fs::remove_dir_all(&dir); // ignore error if doesn't exist
    fs::create_dir_all(&dir)?;
    Ok(())
}

/// A helper to clean up after tests.
pub fn teardown_fs(test_name: &str) {
    let dir = format!("./test_data_{test_name}");
    let _ = fs::remove_dir_all(&dir);
}

use crate::domain::generic::errors::{OpNetError, OpNetResult};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

pub struct WAL {
    file: File,
    current_offset: u64,
}

impl WAL {
    pub fn open<P: AsRef<Path>>(path: P) -> OpNetResult<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| OpNetError::new(&format!("WAL open error: {}", e)))?;

        let current_offset = file
            .seek(SeekFrom::End(0))
            .map_err(|e| OpNetError::new(&format!("WAL seek error: {}", e)))?;
        Ok(WAL {
            file,
            current_offset,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> OpNetResult<()> {
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|e| OpNetError::new(&format!("WAL seek error: {}", e)))?;
        self.file
            .write_all(data)
            .map_err(|e| OpNetError::new(&format!("WAL write error: {}", e)))?;
        self.file
            .flush()
            .map_err(|e| OpNetError::new(&format!("WAL flush error: {}", e)))?;
        self.current_offset = self
            .file
            .seek(SeekFrom::Current(0))
            .map_err(|e| OpNetError::new(&format!("WAL offset error: {}", e)))?;
        Ok(())
    }

    pub fn checkpoint(&mut self) -> OpNetResult<()> {
        self.file
            .sync_data()
            .map_err(|e| OpNetError::new(&format!("WAL sync error: {}", e)))
    }

    pub fn replay(&mut self) -> OpNetResult<()> {
        // read from the beginning, reinsert into memtable if needed
        Ok(())
    }
}

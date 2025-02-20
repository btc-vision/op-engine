use crate::domain::generic::errors::{OpNetError, OpNetResult};

pub struct ReorgManager {
    current_height: u64,
}

impl ReorgManager {
    pub fn new() -> Self {
        Self { current_height: 0 }
    }

    pub fn set_current_height(&mut self, height: u64) {
        self.current_height = height;
    }

    pub fn reorg_to(&mut self, target_height: u64) -> OpNetResult<()> {
        if target_height > self.current_height {
            // In typical Bitcoin logic, you can't reorg to a height above current tip
            return Err(OpNetError::new(
                format!(
                    "Can't reorg to height {} above current height {}",
                    target_height, self.current_height
                )
                .as_str(),
            ));
        }

        self.current_height = target_height;
        Ok(())
    }
}

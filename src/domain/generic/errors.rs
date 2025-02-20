//! errors.rs - Custom error handling

use std::fmt;
use std::error::Error;

pub type OpNetResult<T> = Result<T, OpNetError>;

#[derive(Debug)]
pub struct OpNetError {
    pub msg: String,
}

impl OpNetError {
    pub fn new(msg: &str) -> Self {
        Self { msg: msg.into() }
    }
}

impl fmt::Display for OpNetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OpNetError: {}", self.msg)
    }
}

impl Error for OpNetError {}

impl From<std::io::Error> for OpNetError {
    fn from(e: std::io::Error) -> Self {
        OpNetError::new(&format!("IO Error: {}", e))
    }
}

impl From<String> for OpNetError {
    fn from(s: String) -> Self {
        OpNetError::new(&s)
    }
}

impl From<&str> for OpNetError {
    fn from(s: &str) -> Self {
        OpNetError::new(s)
    }
}

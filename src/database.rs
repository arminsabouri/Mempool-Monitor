use sled::{Db, IVec};
use anyhow::Result;

pub struct Database (sled::Db);

const TX_INDEX_KEY: &[u8; 6] = b"tx_idx";

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self(db))
    }

    pub fn flush(&self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }
}
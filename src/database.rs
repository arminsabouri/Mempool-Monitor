use std::time::SystemTime;

use anyhow::Result;
use bitcoin::{hashes::Hash, Transaction, Txid};
use serde::{Deserialize, Serialize};

pub struct Database(sled::Db);

const TX_INDEX_KEY: &[u8; 6] = b"tx_idx";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransactionInner {
    pub inner: Transaction,
    pub found_at: SystemTime,
    pub mined_at: SystemTime,
}

impl TransactionInner {
    pub(crate) fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            found_at: SystemTime::now(),
            mined_at: SystemTime::UNIX_EPOCH,
        }
    }
}

impl Database {
    pub(crate) fn new(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self(db))
    }

    pub(crate) fn tx_exists(&self, txid: &Txid) -> Result<bool> {
        let key_bytes = txid.as_raw_hash().to_byte_array().to_vec();
        let tx_inner_bytes = self.0.open_tree(TX_INDEX_KEY)?;
        let tx_inner_bytes = tx_inner_bytes.get(key_bytes)?;
        Ok(tx_inner_bytes.is_some())
    }

    pub(crate) fn record_mined_tx(&self, txid: &Txid) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let key_bytes = txid.as_raw_hash().to_byte_array().to_vec();
        let tx_inner_bytes = tree
            .get(key_bytes.clone())?
            .ok_or(anyhow::anyhow!("Transaction not found"))?;
        let mut tx_inner: TransactionInner = bincode::deserialize(&tx_inner_bytes)?;
        tx_inner.mined_at = SystemTime::now();
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;
        tree.insert(&key_bytes, tx_inner_bytes)?;
        self.flush()?;
        Ok(())
    }

    pub(crate) fn insert_mempool_tx(&self, tx: Transaction) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let key_bytes = tx.compute_txid().as_raw_hash().to_byte_array().to_vec();
        let tx_inner = TransactionInner::new(tx);
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;

        tree.insert(&key_bytes, tx_inner_bytes)?;
        self.flush()?;
        Ok(())
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }
}

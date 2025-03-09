use std::time::SystemTime;

use anyhow::Result;
use bitcoin::{consensus::Encodable, hashes::Hash, Transaction, TxIn, Txid};
use bitcoin_hashes::Sha256;
use serde::{Deserialize, Serialize};

pub struct Database(sled::Db);

const TX_INDEX_KEY: &[u8; 6] = b"tx_idx";
const INPUTS_HASH_KEY: &[u8; 7] = b"in_hash";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RBFInner {
    created_at: SystemTime,
    fee_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TransactionInner {
    pub inner: Transaction,
    pub found_at: SystemTime,
    pub mined_at: SystemTime,
    rbf_inner: Vec<RBFInner>,
}

impl TransactionInner {
    pub(crate) fn new(tx: Transaction, found_at: Option<SystemTime>) -> Self {
        Self {
            inner: tx,
            found_at: found_at.unwrap_or(SystemTime::UNIX_EPOCH),
            mined_at: SystemTime::UNIX_EPOCH,
            rbf_inner: vec![],
        }
    }
}

impl Database {
    pub(crate) fn new(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self(db))
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.0.open_tree(INPUTS_HASH_KEY)?.flush()?;
        self.0.open_tree(TX_INDEX_KEY)?.flush()?;
        Ok(())
    }

    pub(crate) fn record_mined_tx(&self, tx: &Transaction) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let inputs_hash = self.get_inputs_hash(tx.clone().input)?;
        let tx_inner_bytes = tree
            .get(inputs_hash.clone())?
            .ok_or(anyhow::anyhow!("Transaction not found"))?;
        let mut tx_inner: TransactionInner = bincode::deserialize(&tx_inner_bytes)?;
        tx_inner.mined_at = SystemTime::now();
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;
        tree.insert(&inputs_hash, tx_inner_bytes)?;
        self.flush()?;

        Ok(())
    }

    pub(crate) fn insert_mempool_tx(
        &self,
        tx: Transaction,
        found_at: Option<SystemTime>,
    ) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let inputs_hash = self.get_inputs_hash(tx.clone().input)?;
        let tx_inner = TransactionInner::new(tx.clone(), found_at);
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;

        tree.insert(&inputs_hash, tx_inner_bytes)?;
        self.flush()?;
        Ok(())
    }

    pub(crate) fn tx_exists(&self, tx: &Transaction) -> Result<bool> {
        let inputs_hash = self.get_inputs_hash(tx.clone().input)?;
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let tx_inner_bytes = tree.get(inputs_hash.clone())?;
        Ok(tx_inner_bytes.is_some())
    }

    pub(crate) fn record_rbf(&self, transaction: Transaction, fee_total: u64) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let inputs_hash = self.get_inputs_hash(transaction.clone().input)?;
        let tx_inner_bytes = tree
            .get(inputs_hash.clone())?
            .ok_or(anyhow::anyhow!("Transaction not found"))?;
        let mut tx_inner: TransactionInner = bincode::deserialize(&tx_inner_bytes)?;
        tx_inner.rbf_inner.push(RBFInner {
            created_at: SystemTime::now(),
            fee_total,
        });
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;
        tree.insert(&inputs_hash, tx_inner_bytes)?;
        self.flush()?;
        Ok(())
    }

    fn get_inputs_hash(&self, inputs: impl IntoIterator<Item = TxIn>) -> Result<Vec<u8>> {
        let mut engine = Sha256::engine();
        for i in inputs {
            let mut writer = vec![];
            i.consensus_encode(&mut writer)
                .expect("encoding doesn't error");
            std::io::copy(&mut writer.as_slice(), &mut engine).expect("engine writes don't error");
        }

        let hash = Sha256::from_engine(engine);
        let hash_bytes = hash.as_byte_array().to_vec();
        Ok(hash_bytes)
    }
}

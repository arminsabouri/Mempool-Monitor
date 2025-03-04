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
    pub(crate) fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            found_at: SystemTime::now(),
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

    pub(crate) fn txid_exists(&self, txid: &Txid) -> Result<bool> {
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
        let tx_inner = TransactionInner::new(tx.clone());
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;

        tree.insert(&key_bytes, tx_inner_bytes)?;
        let inputs_hash = self.get_inputs_hash(tx.input)?;
        let inputs_hash_tree = self.0.open_tree(INPUTS_HASH_KEY)?;
        inputs_hash_tree.insert(&inputs_hash, key_bytes)?;

        self.flush()?;
        Ok(())
    }

    /// Returns the txid of the transaction that has the same inputs as the given transaction.
    pub(crate) fn inputs_hash(
        &self,
        inputs: impl IntoIterator<Item = TxIn>,
    ) -> Result<Option<Txid>> {
        let inputs_hash = self.get_inputs_hash(inputs)?;
        let inputs_hash_tree = self.0.open_tree(INPUTS_HASH_KEY)?;
        let key_bytes = inputs_hash_tree.get(inputs_hash)?;
        if let Some(key_bytes) = key_bytes {
            let byte_array = key_bytes
                .to_vec()
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid key bytes"))?;
            let txid = Txid::from_byte_array(byte_array);
            Ok(Some(txid))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn record_rbf(&self, transaction: Transaction, fee_total: u64) -> Result<()> {
        let tree = self.0.open_tree(TX_INDEX_KEY)?;
        let key_bytes = transaction
            .compute_txid()
            .as_raw_hash()
            .to_byte_array()
            .to_vec();
        let tx_inner_bytes = tree
            .get(key_bytes.clone())?
            .ok_or(anyhow::anyhow!("Transaction not found"))?;
        let mut tx_inner: TransactionInner = bincode::deserialize(&tx_inner_bytes)?;
        tx_inner.rbf_inner.push(RBFInner {
            created_at: SystemTime::now(),
            fee_total,
        });
        let tx_inner_bytes = bincode::serialize(&tx_inner)?;
        tree.insert(&key_bytes, tx_inner_bytes)?;
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
        println!("Inputs hash: {:?}", hash_bytes);
        Ok(hash_bytes)
    }
}

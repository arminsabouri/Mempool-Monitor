use std::{time::SystemTime, vec};

use anyhow::Result;
use bitcoin::{
    consensus::{Decodable, Encodable},
    BlockHash, Transaction, Txid,
};
use hex;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use crate::utils::{get_inputs_hash, get_txid_hex, prune_large_witnesses};

macro_rules! now {
    () => {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
}
#[derive(Debug, Clone)]
pub struct Database(r2d2::Pool<SqliteConnectionManager>);

impl Database {
    pub(crate) fn new(path: &str) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        let conn = pool.get()?;

        // Create tables if they don't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                inputs_hash TEXT PRIMARY KEY,
                tx_id TEXT NOT NULL,
                tx_data TEXT NOT NULL,
                found_at INTEGER NOT NULL,
                mined_at INTEGER,
                pruned_at INTEGER,
                parent_txid TEXT
            )",
            [],
        )?;
        // Create index
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_transactions_tx_id ON transactions(tx_id)",
            [],
        )?;

        // Create the rbf table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rbf (
                inputs_hash TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                fee_total INTEGER NOT NULL
            )",
            [],
        )?;

        // Create the mempool table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mempool (
                tx_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                size INTEGER NOT NULL,
                tx_count INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                block_hash TEXT NOT NULL
            )",
            [],
        )?;

        Ok(Self(pool))
    }

    pub(crate) fn flush(&self) -> Result<()> {
        let conn = self.0.get()?;
        conn.cache_flush()?;
        Ok(())
    }

    pub(crate) fn record_mempool_state(
        &self,
        mempool_size: u64,
        mempool_tx_count: u64,
        block_height: u64,
        block_hash: BlockHash,
    ) -> Result<()> {
        let conn = self.0.get()?;
        let now = now!();
        let mut writer = vec![];
        block_hash.consensus_encode(&mut writer)?;
        let block_hash_str = hex::encode(writer);
        conn.execute(
            "INSERT OR REPLACE INTO mempool (created_at, size, tx_count, block_height, block_hash) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![now, mempool_size, mempool_tx_count, block_height, block_hash_str],
        )?;
        Ok(())
    }

    pub(crate) fn record_coinbase_tx(&self, tx: &Transaction) -> Result<()> {
        let conn = self.0.get()?;
        if !tx.is_coinbase() {
            return Ok(());
        }

        // special case for coinbase tx, key is the txid
        let tx_id = get_txid_hex(&tx.compute_txid());
        let found_at = now!();
        let mined_at = now!();
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_str = hex::encode(tx_bytes);
        conn.execute(
            "INSERT OR REPLACE INTO transactions
            (inputs_hash, tx_data, tx_id, found_at, mined_at)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            params![tx_id, tx_str, tx_id, found_at, mined_at,],
        )?;

        Ok(())
    }

    pub(crate) fn record_mined_tx(&self, tx: &Transaction) -> Result<()> {
        let mut tx = tx.clone();
        prune_large_witnesses(&mut tx);
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_str = hex::encode(tx_bytes);
        let conn = self.0.get()?;
        let mined_at = now!();
        conn.execute(
            "UPDATE transactions SET mined_at = ?1, tx_data = ?2 WHERE inputs_hash = ?3",
            params![mined_at, tx_str, inputs_hash],
        )?;

        Ok(())
    }

    pub(crate) fn txids_of_txs_not_in_list(&self, txids: Vec<Txid>) -> Result<Vec<Txid>> {
        let conn = self.0.get()?;
        if txids.is_empty() {
            let mut stmt = conn.prepare(
                "SELECT tx_id FROM transactions WHERE pruned_at IS NULL AND mined_at IS NULL",
            )?;
            return Ok(stmt
                .query_map([], |row| {
                    let txid_str: String = row.get(0)?;
                    let bytes: Vec<u8> = hex::decode(txid_str).expect("should be valid hex");
                    let txid = Txid::consensus_decode(&mut bytes.as_slice()).expect("Valid txid");

                    Ok(txid)
                })?
                .collect::<Result<Vec<_>, _>>()?);
        }

        let txid_list = txids
            .iter()
            .map(|txid| {
                let mut writer = vec![];
                txid.consensus_encode(&mut writer).expect("Valid txid");
                format!("'{}'", hex::encode(writer))
            })
            .collect::<Vec<String>>()
            .join(",");

        let query = format!(
            "SELECT tx_id FROM transactions WHERE tx_id NOT IN ({}) AND pruned_at IS NULL AND mined_at IS NULL",
            txid_list
        );

        // Uncomment and fix the query execution
        let mut stmt = conn.prepare(&query)?;
        let txids = stmt
            .query_map([], |row| {
                let txid_str: String = row.get(0)?;
                let bytes: Vec<u8> = hex::decode(txid_str).expect("should be valid hex");
                let txid = Txid::consensus_decode(&mut bytes.as_slice()).expect("Valid txid");
                Ok(txid)
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(txids)
    }

    pub(crate) fn record_pruned_txs(&self, txids: Vec<Txid>) -> Result<()> {
        if txids.is_empty() {
            return Ok(());
        }
        let conn = self.0.get()?;
        let pruned_at = now!();
        let txid_list = txids
            .iter()
            .map(|txid| {
                let txid_str = get_txid_hex(txid);
                format!("'{}'", txid_str)
            })
            .collect::<Vec<String>>()
            .join(",");
        println!("txid_list: {}", txid_list);
        let query = format!(
            "UPDATE transactions SET pruned_at = ?1 WHERE tx_id IN ({})",
            txid_list
        );
        let mut stmt = conn.prepare(&query)?;
        stmt.execute(params![pruned_at])?;
        Ok(())
    }

    pub(crate) fn insert_mempool_tx(
        &self,
        tx: Transaction,
        found_at: Option<SystemTime>,
    ) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_str = hex::encode(tx_bytes);

        let tx_id = get_txid_hex(&tx.compute_txid());
        let found_at = found_at
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for input in tx.input.iter() {
            let prev_txid = input.previous_output.txid;
            let parent_txid = get_txid_hex(&prev_txid);
            // Check if txid is in the database
            let txid_exists: bool = conn.query_row(
                "SELECT COUNT(*) FROM transactions WHERE tx_id = ?1",
                params![parent_txid],
                |row| row.get(0),
            )?;
            if txid_exists {
                // Update with parent txid
                conn.execute(
                    "UPDATE transactions SET parent_txid = ?1 WHERE tx_id = ?2",
                    params![tx_id, parent_txid],
                )?;
            }
        }

        conn.execute(
            "INSERT OR REPLACE INTO transactions
            (inputs_hash, tx_id, tx_data, found_at)
            VALUES (?1, ?2, ?3, ?4)",
            params![inputs_hash, tx_id, tx_str, found_at,],
        )?;

        Ok(())
    }

    pub(crate) fn tx_exists(&self, tx: &Transaction) -> Result<bool> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;

        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM transactions WHERE inputs_hash = ?1",
            params![inputs_hash],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    pub(crate) fn record_rbf(&self, transaction: &Transaction, fee_total: u64) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(transaction.clone().input)?;
        let created_at = now!();

        // If input_hash is not in the database, ignore this
        if !self.tx_exists(transaction)? {
            return Ok(());
        }

        conn.execute(
            "INSERT OR REPLACE INTO rbf (inputs_hash, created_at, fee_total) VALUES (?1, ?2, ?3)",
            params![inputs_hash, created_at, fee_total],
        )?;

        Ok(())
    }

    pub(crate) fn update_txid_by_inputs_hash(&self, tx: &Transaction) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let tx_id = get_txid_hex(&tx.compute_txid());
        conn.execute(
            "UPDATE transactions SET tx_id = ?1 WHERE inputs_hash = ?2",
            params![tx_id, inputs_hash],
        )?;

        Ok(())
    }
}

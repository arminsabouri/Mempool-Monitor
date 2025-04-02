use std::{str::FromStr, time::SystemTime, vec};

use anyhow::Result;
use bitcoin::{
    consensus::{Decodable, Encodable},
    hashes::Hash,
    Transaction, Txid,
};
use hex;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use crate::utils::{get_inputs_hash, prune_large_witnesses};

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
                inputs_hash BLOB PRIMARY KEY,
                tx_id BLOB NOT NULL,
                tx_data BLOB NOT NULL,
                found_at INTEGER NOT NULL,
                mined_at INTEGER,
                pruned_at INTEGER,
                mempool_size INTEGER NOT NULL,
                mempool_tx_count INTEGER NOT NULL,
                parent_txid BLOB
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
                inputs_hash BLOB PRIMARY KEY,
                created_at INTEGER NOT NULL,
                fee_total INTEGER NOT NULL
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

    pub(crate) fn record_coinbase_tx(
        &self,
        tx: &Transaction,
        mempool_size: u64,
        mempool_tx_count: u64,
    ) -> Result<()> {
        let conn = self.0.get()?;
        if !tx.is_coinbase() {
            return Ok(());
        }

        // special case for coinbase tx, key is the txid
        let mut key_bytes = vec![];
        tx.compute_txid().consensus_encode(&mut key_bytes)?;
        let tx_id = hex::encode(key_bytes.clone());
        let found_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mined_at = SystemTime::UNIX_EPOCH
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        conn.execute(
            "INSERT OR REPLACE INTO transactions
            (inputs_hash, tx_data, tx_id, found_at, mined_at, mempool_size, mempool_tx_count)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                key_bytes,
                tx_bytes,
                tx_id,
                found_at,
                mined_at,
                mempool_size,
                mempool_tx_count,
            ],
        )?;

        Ok(())
    }

    pub(crate) fn record_mined_tx(&self, tx: &Transaction) -> Result<()> {
        let mut tx = tx.clone();
        prune_large_witnesses(&mut tx);
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;

        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let conn = self.0.get()?;
        let mined_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        conn.execute(
            "UPDATE transactions SET mined_at = ?1, tx_data = ?2 WHERE inputs_hash = ?3",
            params![mined_at, tx_bytes, inputs_hash],
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
        let pruned_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        mempool_size: u64,
        mempool_tx_count: u64,
    ) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_id = tx.compute_txid();
        let mut tx_id_bytes = vec![];
        tx_id.consensus_encode(&mut tx_id_bytes)?;
        let tx_id = hex::encode(tx_id_bytes);
        let found_at = found_at
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for input in tx.input.iter() {
            let parent_txid = input
                .previous_output
                .txid
                .to_raw_hash()
                .as_byte_array()
                .to_vec();
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
            (inputs_hash, tx_id, tx_data, found_at, mempool_size, mempool_tx_count)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                inputs_hash,
                tx_id,
                tx_bytes,
                found_at,
                mempool_size,
                mempool_tx_count
            ],
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

    pub(crate) fn record_rbf(&self, transaction: Transaction, fee_total: u64) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(transaction.clone().input)?;
        let created_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        conn.execute(
            "INSERT OR REPLACE INTO rbf (inputs_hash, created_at, fee_total) VALUES (?1, ?2, ?3)",
            params![inputs_hash, created_at, fee_total],
        )?;

        Ok(())
    }
}

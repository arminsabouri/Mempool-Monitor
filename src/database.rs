use std::{str::FromStr, time::SystemTime, vec};

use anyhow::Result;
use bitcoin::{
    consensus::{Decodable, Encodable},
    Amount, BlockHash, FeeRate, Transaction, Txid,
};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};

use crate::{
    migrations::run_migrations,
    utils::{get_inputs_hash, prune_large_witnesses},
};
use log::info;

#[macro_export]
macro_rules! now {
    () => {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
}

/// Versioning the database, scheme should be backwards compatible
/// But may not always be forwards compatible
const MEMPOOL_TRANSACTION_VERSION: u32 = 1;
const RBF_TRANSACTION_VERSION: u32 = 1;
const COINBASE_TRANSACTION_VERSION: u32 = 0;
const MEMPOOL_STATE_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct Database(r2d2::Pool<SqliteConnectionManager>);

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        let conn = pool.get()?;

        conn.execute("PRAGMA foreign_keys = ON", [])?;

        // Create tables if they don't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                inputs_hash TEXT PRIMARY KEY,
                tx_id TEXT NOT NULL,
                tx_data TEXT NOT NULL,
                found_at DATETIME NOT NULL,
                mined_at DATETIME,
                pruned_at DATETIME,
                parent_txid TEXT,
                absolute_fee INTEGER NOT NULL,
                fee_rate INTEGER NOT NULL,
                version INTEGER NOT NULL
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
                created_at DATETIME NOT NULL,
                fee_total INTEGER NOT NULL,
                version INTEGER NOT NULL
            )",
            [],
        )?;

        // Create the mempool table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mempool (
                tx_id TEXT PRIMARY KEY,
                created_at DATETIME NOT NULL,
                size INTEGER NOT NULL,
                tx_count INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                block_hash TEXT NOT NULL,
                version INTEGER NOT NULL
            )",
            [],
        )?;

        // Migrations table tracking what migrations have been applied
        conn.execute(
            "CREATE TABLE IF NOT EXISTS migrations (
                id TEXT PRIMARY KEY,
                applied_at DATETIME NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS mining_info (
                created_at DATETIME NOT NULL,
                hash_rate_distribution TEXT NOT NULL
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
            "INSERT OR REPLACE INTO mempool (created_at, size, tx_count, block_height, block_hash, version) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![now, mempool_size, mempool_tx_count, block_height, block_hash_str, MEMPOOL_STATE_VERSION],
        )?;
        Ok(())
    }

    pub(crate) fn record_coinbase_tx(&self, tx: &Transaction) -> Result<()> {
        let conn = self.0.get()?;
        if !tx.is_coinbase() {
            return Ok(());
        }

        // special case for coinbase tx, key is the txid
        let tx_id = tx.compute_txid().to_string();
        let found_at = now!();
        let mined_at = now!();
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_str = hex::encode(tx_bytes);
        conn.execute(
            "INSERT OR REPLACE INTO transactions
            (inputs_hash, tx_data, tx_id, found_at, mined_at, absolute_fee, fee_rate, version)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                tx_id,
                tx_str,
                tx_id,
                found_at,
                mined_at,
                Amount::ZERO.to_sat(),
                FeeRate::ZERO.to_sat_per_vb_ceil(),
                COINBASE_TRANSACTION_VERSION
            ],
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

        let tx_in_mempool = self.tx_exists(&tx)?;
        if !tx_in_mempool {
            info!("Received tx that was not in my mempool: {}", inputs_hash);
        }
        conn.execute(
            "UPDATE transactions SET mined_at = ?1, tx_data = ?2, seen_in_mempool = ?3 WHERE inputs_hash = ?4",
            params![mined_at, tx_str, tx_in_mempool, inputs_hash],
        )?;

        Ok(())
    }

    pub(crate) fn txids_in_mempool(&self) -> Result<Vec<Txid>> {
        let conn = self.0.get()?;
        let query = "SELECT tx_id FROM transactions WHERE pruned_at IS NULL AND mined_at IS NULL";
        let mut stmt = conn.prepare(query)?;
        let txids = stmt.query_map([], |row| {
            let txid_str: String = row.get(0)?;
            let txid = Txid::from_str(&txid_str).expect("Valid txid");
            Ok(txid)
        })?;
        Ok(txids.collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn txids_of_txs_not_in_list(&self, txids: Vec<Txid>) -> Result<Vec<Txid>> {
        let mempool_txids = self.txids_in_mempool()?;
        // If mempool is empty, don't mark anything as pruned
        // This could be a temporary state or network issue
        // We dont want to mark all txs as pruned
        if txids.is_empty() {
            return Ok(vec![]);
        }

        let txids_not_in_current_mempool = mempool_txids
            .iter()
            .filter(|txid| !txids.contains(txid))
            .copied()
            .collect::<Vec<_>>();

        Ok(txids_not_in_current_mempool)
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
                let txid_str = txid.to_string();
                format!("'{}'", txid_str)
            })
            .collect::<Vec<String>>()
            .join(",");
        info!("txid_list: {}", txid_list);
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
        found_at: Option<u64>,
        absolute_fee: Amount,
        fee_rate: FeeRate,
    ) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let mut tx_bytes = vec![];
        tx.consensus_encode(&mut tx_bytes)?;
        let tx_str = hex::encode(tx_bytes);

        let tx_id = tx.compute_txid().to_string();
        let found_at = found_at.unwrap_or(now!());

        for input in tx.input.iter() {
            let prev_txid = input.previous_output.txid;
            let parent_txid = prev_txid.to_string();
            // Check if txid is in the database
            let txid_exists: bool = conn.query_row(
                "SELECT COUNT(*) FROM transactions WHERE tx_id = ?1 AND mined_at is NULL AND pruned_at is NULL",
                params![parent_txid],
                |row| row.get(0),
            )?;
            if txid_exists {
                // Update with parent txid and mark as CPFP parent
                conn.execute(
                    "UPDATE transactions SET child_txid = ?1, is_cpfp_parent = TRUE WHERE tx_id = ?2",
                    params![tx_id, parent_txid],
                )?;
            }
        }

        conn.execute(
            "INSERT OR REPLACE INTO transactions
            (inputs_hash, tx_id, tx_data, found_at, absolute_fee, fee_rate, version)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                inputs_hash,
                tx_id,
                tx_str,
                found_at,
                absolute_fee.to_sat(),
                fee_rate.to_sat_per_vb_ceil(),
                MEMPOOL_TRANSACTION_VERSION
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

    pub(crate) fn record_rbf(
        &self,
        tx: &Transaction,
        fee_total: u64,
        // TODO: Store the fee rate bump
        _fee_rate: FeeRate,
    ) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;

        // If input_hash is not in the database, ignore this
        if !self.tx_exists(tx)? {
            info!("Replaced Tx not found in database, ignoring RBF");
            return Ok(());
        }

        // Insert new tx into rbf table
        let txid = tx.compute_txid().to_string();
        conn.execute(
            "INSERT OR REPLACE INTO rbf (inputs_hash, created_at, fee_total, replaces, version) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![inputs_hash, now!(), fee_total, txid, RBF_TRANSACTION_VERSION],
        )?;

        Ok(())
    }

    pub(crate) fn update_txid_by_inputs_hash(&self, tx: &Transaction) -> Result<()> {
        let conn = self.0.get()?;
        let inputs_hash = get_inputs_hash(tx.clone().input)?;
        let tx_id = tx.compute_txid().to_string();
        conn.execute(
            "UPDATE transactions SET tx_id = ?1 WHERE inputs_hash = ?2",
            params![tx_id, inputs_hash],
        )?;

        Ok(())
    }

    pub(crate) fn record_mining_info(&self, hash_rate_distribution: String) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "INSERT OR REPLACE INTO mining_info (created_at, hash_rate_distribution) VALUES (?1, ?2)",
            params![now!(), hash_rate_distribution],
        )?;

        Ok(())
    }

    /// Remove txs that are neither pruned nor mined
    /// This should be called when the system if first started
    /// As the db may include old txs that have been pruned or mined
    pub(crate) fn remove_stale_txs(&self) -> Result<()> {
        let conn = self.0.get()?;
        conn.execute(
            "DELETE FROM transactions WHERE pruned_at IS NULL AND mined_at IS NULL",
            [],
        )?;
        Ok(())
    }

    pub(crate) fn run_migrations(&self) -> Result<()> {
        let conn = self.0.get()?;
        run_migrations(&conn)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_tx_by_txid(&self, txid: &Txid) -> Result<Option<Transaction>> {
        let conn = self.0.get()?;
        let txid_hex = txid.to_string();
        let mut stmt = conn.prepare("SELECT tx_data FROM transactions WHERE tx_id = ?1")?;
        let tx_data: Option<String> = stmt
            .query_row(params![txid_hex], |row| row.get(0))
            .optional()?;

        Ok(tx_data.map(|data| {
            let bytes = hex::decode(data).expect("should be valid hex");
            Transaction::consensus_decode(&mut bytes.as_slice()).expect("Valid transaction")
        }))
    }

    /// Check if a transaction is marked as a CPFP parent
    #[allow(dead_code)]
    pub fn is_cpfp_parent(&self, txid: &Txid) -> Result<bool> {
        let conn = self.0.get()?;
        let txid_hex = txid.to_string();
        let is_parent: bool = conn.query_row(
            "SELECT is_cpfp_parent FROM transactions WHERE tx_id = ?1",
            params![txid_hex],
            |row| row.get(0),
        )?;
        Ok(is_parent)
    }

    /// Check if a transaction is mined
    #[allow(dead_code)]
    pub fn is_mined(&self, txid: &Txid) -> Result<bool> {
        let conn = self.0.get()?;
        let txid_hex = txid.to_string();
        let mined_at: Option<u64> = conn.query_row(
            "SELECT mined_at FROM transactions WHERE tx_id = ?1",
            params![txid_hex],
            |row| row.get(0),
        )?;
        Ok(mined_at.is_some())
    }

    /// Check if a transaction is in the RBF table
    #[allow(dead_code)]
    pub fn is_rbf(&self, txid: &Txid) -> Result<bool> {
        let conn = self.0.get()?;
        let txid_hex = txid.to_string();
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM rbf WHERE replaces = ?1",
            params![txid_hex],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }
}

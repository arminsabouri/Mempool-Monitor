use crate::{
    database::Database,
    utils::{compute_fee_rate, get_hash_rate_distribution},
};
use anyhow::Result;
use async_channel::Receiver;
use bitcoin::{consensus::Decodable, Amount, Transaction};
use bitcoind_async_client::{traits::Reader, Client};
use log::{debug, error, info};

// Macro to execute a function, if its error, log it and continue
macro_rules! log_error {
    ($fn:expr, $arg:expr) => {
        if let Err(e) = $fn($arg).await {
            error!("Error: {}", e);
            continue;
        }
    };
    ($fn:expr, $arg1:expr, $arg2:expr) => {
        if let Err(e) = $fn($arg1, $arg2) {
            error!("Error: {}", e);
            continue;
        }
    };
    ($fn:expr, $arg1:expr, $arg2:expr, $arg3:expr) => {
        if let Err(e) = $fn($arg1, $arg2, $arg3) {
            error!("Error: {}", e);
            continue;
        }
    };
}
#[derive(Debug, Clone)]
pub enum Task {
    RawTx(Vec<u8>),
    PruneCheck,
    MempoolState,
    MiningInfo,
}

pub struct TaskContext {
    bitcoind: Client,
    db: Database,
    tasks: Receiver<Task>,
}

/// Return absolute fee of a transaction
pub async fn get_absolute_fee(tx: &Transaction, rpc_client: &Client) -> Result<Amount> {
    if tx.is_coinbase() {
        return Ok(Amount::ZERO);
    }
    let mut input_value = Amount::from_sat(0);
    for vin in tx.input.iter() {
        if vin.previous_output.is_null() {
            continue;
        }
        debug!("Getting input tx: {:?}", vin.previous_output.txid);
        let prev_tx = rpc_client
            .get_raw_transaction_verbosity_zero(&vin.previous_output.txid)
            .await?
            .transaction()?;
        let prev_txout = prev_tx.output[vin.previous_output.vout as usize].clone();
        let prev_txout_value = prev_txout.value;
        input_value += prev_txout_value;
    }
    let output_value = tx.output.iter().map(|vout| vout.value).sum();
    let fee = input_value - output_value;
    Ok(fee)
}

impl TaskContext {
    pub fn new(bitcoind: Client, db: Database, tasks: Receiver<Task>) -> Self {
        Self {
            bitcoind,
            db,
            tasks,
        }
    }

    async fn check_for_pruned_txs(&self) -> Result<()> {
        info!("Checking for pruned txs");
        let txids = self.bitcoind.get_raw_mempool().await?;
        let db = self.db.clone();
        let pruned_txids = tokio::task::spawn_blocking(move || {
            db.txids_of_txs_not_in_list(txids)
        })
        .await??;
        info!("Found {} pruned txs", pruned_txids.len());
        self.db.record_pruned_txs(pruned_txids)?;
        self.db.flush()?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Ok(task) = self.tasks.recv().await {
            match task {
                Task::MiningInfo => {
                    info!("Mining info task received");
                    let hash_rate_distribution = get_hash_rate_distribution().await?;
                    info!("Hash rate distribution: {}", hash_rate_distribution);
                    self.db.record_mining_info(hash_rate_distribution)?;
                    self.db.flush()?;
                }
                Task::MempoolState => {
                    info!("Mempool state task received");
                    let mempool_info = self.bitcoind.get_mempool_info().await?;
                    let block_height = self.bitcoind.get_block_count().await?;
                    let block_hash = self.bitcoind.get_block_hash(block_height).await?;
                    if let Err(e) = self.db.record_mempool_state(
                        mempool_info.bytes as u64,
                        mempool_info.size as u64,
                        block_height,
                        block_hash,
                    ) {
                        error!("Error recording mempool state: {}", e);
                        continue;
                    }
                }
                Task::PruneCheck => {
                    info!("Prune check task received");
                    log_error!(Self::check_for_pruned_txs, self);
                }
                Task::RawTx(raw_tx) => {
                    debug!("Received raw tx");
                    let tx_bytes = raw_tx;
                    let tx = Transaction::consensus_decode(&mut tx_bytes.as_slice())?;
                    if tx.is_coinbase() {
                        info!("Record coinbase tx");
                        // Record coinbase sperately
                        self.db.record_coinbase_tx(&tx)?;
                        continue;
                    }

                    let txid = tx.compute_txid();
                    let tx_info = match self.bitcoind.get_raw_transaction_verbosity_one(&txid).await
                    {
                        Ok(tx_info) => tx_info,
                        Err(e) => {
                            error!("Error getting transaction info: {}", e);
                            continue;
                        }
                    };
                    let is_mined = tx_info.confirmations.unwrap_or(0) > 0;
                    let fee = match get_absolute_fee(&tx, &self.bitcoind).await {
                        Ok(fee) => fee,
                        Err(e) => {
                            error!("Error getting transaction fee: {}", e);
                            continue;
                        }
                    };
                    let fee_rate = match compute_fee_rate(&tx, fee) {
                        Ok(fee_rate) => fee_rate,
                        Err(e) => {
                            error!("Error computing fee rate: {}", e);
                            continue;
                        }
                    };

                    if is_mined {
                        self.db.record_mined_tx(&tx)?;
                        info!("Transaction was mined: {:?}", txid);
                        continue;
                    }

                    if self.db.tx_exists(&tx)? {
                        info!("Transaction was RBF'd: {:?}", txid);
                        self.db.record_rbf(&tx, fee.to_sat(), fee_rate)?;
                        self.db.update_txid_by_inputs_hash(&tx)?;
                        continue;
                    }

                    self.db.insert_mempool_tx(tx, None, fee, fee_rate)?;
                    self.db.flush()?;
                    info!("Transaction inserted: {:?}", txid);
                }
            }
        }
        info!("Worker shutting down");
        Ok(())
    }
}

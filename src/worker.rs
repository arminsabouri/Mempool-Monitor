use crate::database::Database;
use anyhow::Result;
use async_channel::Receiver;
use bitcoin::{consensus::Decodable, Amount, Transaction};
use bitcoind::bitcoincore_rpc::{Client, RpcApi};
use log::{debug, info};

#[derive(Debug, Clone)]
pub enum Task {
    RawTx(Vec<u8>),
    PruneCheck,
    MempoolState,
}

pub struct TaskContext {
    bitcoind: Client,
    db: Database,
    tasks: Receiver<Task>,
}

impl TaskContext {
    pub fn new(bitcoind: Client, db: Database, tasks: Receiver<Task>) -> Self {
        Self {
            bitcoind,
            db,
            tasks,
        }
    }

    fn get_transaction_fee(&self, tx: &Transaction) -> Result<Amount> {
        let tx = self.bitcoind.get_mempool_entry(&tx.compute_txid())?;
        Ok(tx.fees.base)
    }

    fn check_for_pruned_txs(&self) -> Result<()> {
        info!("Checking for pruned txs");
        let txids = self.bitcoind.get_raw_mempool()?;
        let pruned_txids = self.db.txids_of_txs_not_in_list(txids)?;
        info!("Found {} pruned txs", pruned_txids.len());
        self.db.record_pruned_txs(pruned_txids)?;
        self.db.flush()?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Ok(task) = self.tasks.recv().await {
            match task {
                Task::MempoolState => {
                    info!("Mempool state task received");
                    let mempool_info = self.bitcoind.get_mempool_info()?;
                    let block_height = self.bitcoind.get_block_count()?;
                    let block_hash = self.bitcoind.get_block_hash(block_height)?;
                    self.db.record_mempool_state(
                        mempool_info.bytes as u64,
                        mempool_info.size as u64,
                        block_height,
                        block_hash,
                    )?;
                }
                Task::PruneCheck => {
                    info!("Prune check task received");
                    self.check_for_pruned_txs()?;
                }
                Task::RawTx(raw_tx) => {
                    debug!("Received raw tx");
                    let tx_bytes = raw_tx;
                    let tx = Transaction::consensus_decode(&mut tx_bytes.as_slice())?;
                    if tx.is_coinbase() {
                        info!("Record coinbase tx");
                        // Record coinbase sperately
                        self.db.record_coinbase_tx(&tx)?;
                        self.db.flush()?;
                        continue;
                    }

                    let txid = tx.compute_txid();
                    let tx_info = self.bitcoind.get_raw_transaction_info(&txid, None)?;
                    let is_mined = tx_info.confirmations.unwrap_or(0) > 0;

                    if self.db.tx_exists(&tx)? {
                        if is_mined {
                            self.db.record_mined_tx(&tx)?;
                            info!("Transaction was mined: {:?}", txid);
                        } else {
                            info!("Transaction was RBF'd: {:?}", txid);
                            let fee = self.get_transaction_fee(&tx)?;
                            debug!("Fee: {}", fee);
                            self.db.record_rbf(&tx, fee.to_sat())?;
                            self.db.update_txid_by_inputs_hash(&tx)?;
                        }
                        self.db.flush()?;
                        continue;
                    }

                    self.db.insert_mempool_tx(tx, None)?;
                    self.db.flush()?;
                    info!("Transaction inserted: {:?}", txid);
                }
            }
        }
        info!("Worker shutting down");
        Ok(())
    }
}

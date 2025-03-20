use std::sync::Arc;

use crate::database::Database;
use anyhow::Result;
use async_channel::Receiver;
use bitcoin::{consensus::Decodable, Amount, Transaction};
use bitcoind::bitcoincore_rpc::{Client, RpcApi};
use log::info;
use tokio::sync::RwLock;

pub struct TaskContext {
    bitcoind: Client,
    db: Database,
    _lock: Arc<RwLock<()>>,
    raw_txs_rx: Receiver<Vec<u8>>,
}

impl TaskContext {
    pub fn new(
        bitcoind: Client,
        db: Database,
        _lock: Arc<RwLock<()>>,
        raw_txs_rx: Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            bitcoind,
            db,
            _lock,
            raw_txs_rx,
        }
    }

    fn get_transaction_fee(&self, tx: &Transaction) -> Result<Amount> {
        // TODO: this should be handled on a non blocking thread. This could halt if there are many inputs
        let out_total = tx.output.iter().map(|o| o.value).sum::<Amount>();
        let mut in_total = Amount::ZERO;
        for input in tx.input.iter() {
            // TODO: handle coinbase inputs
            let ot = input.previous_output;
            let prev_tx = self.bitcoind.get_transaction(&ot.txid, None)?;
            let prev_out = prev_tx.transaction()?.output[ot.vout as usize].value;
            in_total += prev_out;
        }
        Ok(in_total - out_total)
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Ok(raw_tx) = self.raw_txs_rx.recv().await {
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
                    info!("Transaction already exists: {:?}", txid);
                } else {
                    info!("Transaction was RBF'd: {:?}", txid);
                    let fee = self.get_transaction_fee(&tx)?;
                    info!("Fee: {}", fee);
                    self.db.record_rbf(tx, fee.to_sat())?;
                }
                self.db.flush()?;
                continue;
            }

            self.db.insert_mempool_tx(tx, None)?;
            self.db.flush()?;
            info!("Transaction inserted: {:?}", txid);
        }
        Ok(())
    }
}

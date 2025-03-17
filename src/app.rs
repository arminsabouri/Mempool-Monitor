use std::time::{Duration, SystemTime};

use crate::database::Database;
use anyhow::Result;
use bitcoin::{consensus::Decodable, Amount, Transaction};
use bitcoincore_zmq::MessageStream;
use bitcoind::bitcoincore_rpc::{Client, RpcApi};
use futures_util::StreamExt;
use log::info;

pub struct App {
    bitcoind: Client,
    zmq: MessageStream,
    db: Database,
}

impl App {
    pub fn new(bitcoind: Client, zmq: MessageStream, db: Database) -> Self {
        Self { bitcoind, zmq, db }
    }

    fn extract_existing_mempool(&self) -> Result<()> {
        let mempool = self.bitcoind.get_raw_mempool_verbose()?;
        info!("Found {} transactions in mempool", mempool.len());

        for (txid, mempool_tx) in mempool.iter() {
            let pool_exntrance_time = mempool_tx.time;
            let tx = self.bitcoind.get_transaction(txid, None)?.transaction()?;
            let found_at = SystemTime::UNIX_EPOCH + Duration::from_secs(pool_exntrance_time as u64);
            self.db.insert_mempool_tx(tx, Some(found_at))?;
        }

        Ok(())
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

    pub fn init(&mut self) -> Result<()> {
        self.extract_existing_mempool()?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting mempool tracker");
        while let Some(message) = self.zmq.next().await {
            match message {
                Ok(message) => {
                    let topic = message.topic_str();
                    info!("Received message: {}", topic);

                    let tx_bytes = message.serialize_data_to_vec();
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
                        continue;
                    }

                    self.db.insert_mempool_tx(tx, None)?;
                    info!("Transaction inserted: {:?}", txid);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

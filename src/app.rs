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
        info!("Starting mempool tracker");
        while let Some(message) = self.zmq.next().await {
            match message {
                Ok(message) => {
                    let topic = message.topic_str();
                    info!("Received message: {}", topic);

                    let tx_bytes = message.serialize_data_to_vec();
                    let tx = Transaction::consensus_decode(&mut tx_bytes.as_slice())?;
                    let txid = tx.compute_txid();
                    if let Some(txid) = self.db.inputs_hash(tx.clone().input)? {
                        info!("Transaction was RBF'd: {:?}", txid);
                        let fee = self.get_transaction_fee(&tx)?;
                        info!("Fee: {}", fee);
                        self.db.record_rbf(tx, fee.to_sat())?;
                        continue;
                    }

                    if self.db.txid_exists(&txid)? {
                        self.db.record_mined_tx(&txid)?;
                        info!("Transaction already exists: {:?}", txid);
                        continue;
                    }
                    self.db.insert_mempool_tx(tx)?;
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

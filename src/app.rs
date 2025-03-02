use crate::database::Database;
use anyhow::Result;
use bitcoin::{consensus::Decodable, Transaction};
use bitcoincore_zmq::MessageStream;
use bitcoind::bitcoincore_rpc::Client;
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
                    if self.db.tx_exists(&txid)? {
                        self.db.record_mined_tx(&txid)?;
                        info!("Transaction already exists: {:?}", txid);
                    } else {
                        self.db.insert_mempool_tx(tx)?;
                        info!("Transaction inserted: {:?}", txid);
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

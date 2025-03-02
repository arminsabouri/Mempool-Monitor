use bitcoincore_zmq::MessageStream;
use bitcoind::bitcoincore_rpc::Client;
use futures_util::StreamExt;
use log::info;
use crate::database::Database;
use anyhow::Result;

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
                    let topic  =message.topic_str();
                    info!("Received message: {}", topic);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

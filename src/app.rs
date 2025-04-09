use std::time::{Duration, SystemTime};

use crate::{
    database::Database,
    worker::{Task, TaskContext},
    BitcoinZmqFactory,
};
use anyhow::Result;
use async_channel::{bounded, Receiver, Sender};
use bitcoind::bitcoincore_rpc::{Auth, Client, RpcApi};
use futures_util::StreamExt;
use log::info;
use tokio::signal::ctrl_c;

fn connect_bitcoind(bitcoind_host: &str, bitcoind_auth: Auth) -> Result<Client> {
    let bitcoind = Client::new(bitcoind_host, bitcoind_auth)?;
    Ok(bitcoind)
}

#[derive(Debug)]
pub struct App {
    zmq_factory: BitcoinZmqFactory,
    db: Database,
    tasks_tx: Sender<Task>,
    tasks_rx: Receiver<Task>,
    bitcoind_url: String,
    bitcoind_auth: Auth,
    num_workers: usize,
}

impl App {
    pub fn new(
        bitcoind_url: String,
        bitcoind_auth: Auth,
        zmq_factory: BitcoinZmqFactory,
        db: Database,
        num_workers: usize,
    ) -> Self {
        let (sender, receiver) = bounded(100_000);
        Self {
            bitcoind_url,
            bitcoind_auth,
            zmq_factory,
            db,
            tasks_tx: sender,
            tasks_rx: receiver,
            num_workers,
        }
    }

    fn extract_existing_mempool(&self) -> Result<()> {
        let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
        let mempool = bitcoind.get_raw_mempool_verbose()?;
        info!("Found {} transactions in mempool", mempool.len());

        for (txid, mempool_tx) in mempool.iter() {
            let pool_entrance_time = mempool_tx.time;
            let tx = bitcoind
                .get_raw_transaction_info(txid, None)?
                .transaction()?;
            let found_at = SystemTime::UNIX_EPOCH + Duration::from_secs(pool_entrance_time);
            self.db.insert_mempool_tx(tx, Some(found_at))?;
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<()> {
        self.extract_existing_mempool()?;
        let mut task_handles = vec![];
        for _ in 0..self.num_workers {
            let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
            let mut task_context =
                TaskContext::new(bitcoind, self.db.clone(), self.tasks_rx.clone());
            task_handles.push(tokio::spawn(async move { task_context.run().await }));
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("===== Starting mempool tracker =====");
        let tasks_tx = self.tasks_tx.clone();
        let tasks_tx_2 = self.tasks_tx.clone();
        let tasks_tx_3 = self.tasks_tx.clone();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let shutdown_rx_1 = shutdown_tx.subscribe();
        let shutdown_rx_2 = shutdown_tx.subscribe();
        let shutdown_rx_3 = shutdown_tx.subscribe();

        let mempool_state_handle = tokio::spawn(async move {
            let mut shutdown = shutdown_rx_1;
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("Shutting down mempool state task");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {
                        tasks_tx.send(Task::MempoolState).await?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let prune_check_handle = tokio::spawn(async move {
            let mut shutdown = shutdown_rx_2;
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("Shutting down prune check task");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        tasks_tx_2.send(Task::PruneCheck).await?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let mut zmq_message_stream = self.zmq_factory.connect()?;
        let zmq_handle = {
            let mut shutdown = shutdown_rx_3;
            tokio::spawn(async move {
                info!("Starting zmq handle");
                loop {
                    tokio::select! {
                        _ = shutdown.recv() => {
                            info!("Shutting down zmq handle");
                            break;
                        }
                        message = zmq_message_stream.next() => {
                            match message {
                                Some(Ok(message)) => {
                                    tasks_tx_3.send(Task::RawTx(message.serialize_data_to_vec())).await?;
                                }
                                Some(Err(e)) => return Err(e.into()),
                                None => break,
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        };

        // Wait for ctrl-c
        tokio::select! {
            _ = ctrl_c() => {
                info!("Received shutdown signal");
                shutdown_tx.send(())?;
            }
            r = mempool_state_handle => r??,
            r = prune_check_handle => r??,
            r = zmq_handle => r??,
        }

        // Clean up
        info!("Shutting down workers...");
        self.tasks_tx.close();
        self.db.flush()?;
        info!("Shutdown complete");

        Ok(())
    }
}

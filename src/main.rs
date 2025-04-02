use anyhow::Result;
use bitcoincore_zmq::{self, MessageStream};
use bitcoind::bitcoincore_rpc::Auth;
use clap::Parser;

mod app;
mod database;
mod utils;
mod worker;

// Command line arguments
#[derive(Clone, Debug, Parser)]
struct Args {
    #[clap(long)]
    bitcoind_user: String,
    #[clap(long)]
    bitcoind_password: String,
    #[clap(long)]
    bitcoind_host: String,
    #[clap(long)]
    bitcoind_rpc_port: u16,
    #[clap(long)]
    bitcoind_zmq_port: u16,
}

#[derive(Debug, Clone)]
pub struct BitcoinZmqFactory {
    bitcoind_host: String,
    bitcoind_zmq_port: u16,
}

impl BitcoinZmqFactory {
    pub fn new(bitcoind_host: String, bitcoind_zmq_port: u16) -> Self {
        Self {
            bitcoind_host,
            bitcoind_zmq_port,
        }
    }

    pub fn connect(&self) -> Result<MessageStream> {
        let zmq = bitcoincore_zmq::subscribe_async(&[&format!(
            "tcp://{}:{}",
            self.bitcoind_host, self.bitcoind_zmq_port
        )])?;
        Ok(zmq)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("welcome to mempool tracker");
    env_logger::init();

    let args = Args::parse();
    let zmq_factory =
        BitcoinZmqFactory::new(args.bitcoind_host.clone(), args.bitcoind_zmq_port.clone());
    let db = database::Database::new("mempool-tracker.db")?;
    let auth = Auth::UserPass(args.bitcoind_user, args.bitcoind_password);
    let bitcoind_url = format!("http://{}:{}", args.bitcoind_host, args.bitcoind_rpc_port);
    let mut app = app::App::new(bitcoind_url, auth, zmq_factory, db);

    app.init()?;
    app.run().await?;

    Ok(())
}

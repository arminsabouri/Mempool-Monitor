use std::time::Duration;

use anyhow::Result;
use bitcoind_async_client::Client;
use clap::Parser;
use zmq_factory::BitcoinZmqFactory;

mod app;
mod database;
mod migrations;
mod utils;
mod worker;
mod zmq_factory;

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
    #[clap(long, default_value_t = 2)]
    num_workers: u32,
    #[clap(long, default_value_t = 25)]
    mempool_state_check_interval: u64,
    #[clap(long, default_value_t = 120)]
    prune_check_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("welcome to mempool tracker");
    env_logger::init();

    let args = Args::parse();
    let zmq_factory =
        BitcoinZmqFactory::new(args.bitcoind_host.clone(), args.bitcoind_zmq_port.clone());
    let db = database::Database::new("mempool-tracker.db")?;
    let bitcoind_url = format!("http://{}:{}", args.bitcoind_host, args.bitcoind_rpc_port);

    // parse u64 to duration
    // TODO: add some validation
    let mempool_state_check_interval = Duration::from_secs(args.mempool_state_check_interval);
    let prune_check_interval = Duration::from_secs(args.prune_check_interval);

    let rpc_client = Client::new(
        bitcoind_url,
        args.bitcoind_user,
        args.bitcoind_password,
        None,
        None,
    )?;
    let mut app = app::App::new(
        rpc_client,
        zmq_factory,
        db,
        args.num_workers as usize,
        mempool_state_check_interval,
        prune_check_interval,
    );
    app.init().await?;
    app.run().await?;

    Ok(())
}

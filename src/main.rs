use anyhow::Result;
use bitcoincore_zmq::{self, MessageStream};
use bitcoind::bitcoincore_rpc::Auth;
use clap::Parser;

mod app;
mod database;
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

fn connect_zmq(args: &Args) -> Result<MessageStream> {
    let zmq = bitcoincore_zmq::subscribe_async(&[&format!(
        "tcp://{}:{}",
        args.bitcoind_host, args.bitcoind_zmq_port
    )])?;
    Ok(zmq)
}

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("welcome to mempool tracker");
    env_logger::init();

    let args = Args::parse();
    let zmq = connect_zmq(&args)?;
    let db = database::Database::new("mempool-tracker.db")?;
    let auth = Auth::UserPass(args.bitcoind_user, args.bitcoind_password);
    let bitcoind_url = format!("http://{}:{}", args.bitcoind_host, args.bitcoind_rpc_port);
    let mut app = app::App::new(bitcoind_url, auth, zmq, db);
    app.init()?;
    app.run().await?;

    Ok(())
}

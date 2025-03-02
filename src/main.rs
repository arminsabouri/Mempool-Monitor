use bitcoind::bitcoincore_rpc::{Auth, Client};
use clap::Parser;
use anyhow::Result;
use bitcoincore_zmq::{self, MessageStream};

mod app;
mod database;

// Command line arguments
#[derive(Clone, Debug, Parser)]
struct Args {
    // The path to the file to read
    #[clap(long,)]
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

fn connect_bitcoind(args: &Args) -> Result<Client> {
    let bitcoind = Client::new(
        &format!("http://{}:{}", args.bitcoind_host, args.bitcoind_rpc_port),
        Auth::UserPass(args.bitcoind_user.clone(), args.bitcoind_password.clone()),
    )?;
    Ok(bitcoind)
}

fn connect_zmq(args: &Args) -> Result<MessageStream> {
    let zmq = bitcoincore_zmq::subscribe_async(
        &[&format!("tcp://{}:{}", args.bitcoind_host, args.bitcoind_zmq_port)],
    )?;
    Ok(zmq)
}

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("welcome to mempool tracker");
    env_logger::init();

    let args = Args::parse();
    let bitcoind = connect_bitcoind(&args)?;
    let zmq = connect_zmq(&args)?;
    let db = database::Database::new("mempool-tracker.db")?;
    let mut app = app::App::new(bitcoind, zmq, db);
    app.run().await?;

    Ok(())
}

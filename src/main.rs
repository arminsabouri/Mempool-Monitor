use anyhow::Result;
use bitcoind::bitcoincore_rpc::{Auth, Client};
use clap::Parser;

// Command line arguments
#[derive(Clone, Debug, Parser)]
struct Args {
    // The path to the file to read
    #[clap(short, long)]
    bitcoind_user: String,
    #[clap(short, long)]
    bitcoind_password: String,
    #[clap(short, long)]
    bitcoind_host: String,
    #[clap(short, long)]
    bitcoind_rpc_port: u16,
    #[clap(short, long)]
    bitcoind_zmq_port: u16,
}

fn connect_bitcoind(args: &Args) -> Result<Client> {
    let bitcoind = Client::new(
        &format!("http://{}:{}", args.bitcoind_host, args.bitcoind_rpc_port),
        Auth::UserPass(args.bitcoind_user.clone(), args.bitcoind_password.clone()),
    )?;
    Ok(bitcoind)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bitcoind = connect_bitcoind(&args)?;

    Ok(())
}

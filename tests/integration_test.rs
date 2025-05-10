use anyhow::Result;
use bitcoin::Amount;
use bitcoind::bitcoincore_rpc::{Auth, Client, RpcApi};
use mempool_tracker::{app::App, database::Database, zmq_factory::BitcoinZmqFactory};
use std::path::PathBuf;
use std::time::Duration;
const RPC_HOST: &str = "127.0.0.1";
const RPC_PORT: u16 = 18443;
const ZMQ_PORT: u16 = 28373;
const RPC_USER: &str = "foo";
const RPC_PASS: &str = "bar";

struct TestContext {
    bitcoind: Client,
    db: Database,
    app: App,
    db_path: PathBuf,
}

impl TestContext {
    async fn setup() -> Result<Self> {
        // Create temp directory for test database
        let temp_dir = std::path::PathBuf::from("./");
        let db_path = temp_dir.as_path().join("mempool_tracker_test.db");

        // Setup Bitcoin RPC client
        let auth = Auth::UserPass(RPC_USER.to_string(), RPC_PASS.to_string());
        let url = format!("http://{}:{}", RPC_HOST, RPC_PORT);
        let bitcoind = Client::new(&url, auth.clone())?;

        // Create and fund test wallet
        let wallet_name = "mempool_tracker_wallet";
        match bitcoind.create_wallet(&wallet_name, None, None, None, None) {
            Ok(_) => (),
            Err(e) => {
                if e.to_string().contains("already exists") {
                    println!("wallet already exists");
                    // load wallet
                    let _ = bitcoind.load_wallet(&wallet_name);
                } else {
                    panic!("failed to create wallet: {}", e);
                }
            }
        }

        let url = format!("http://{}:{}/wallet/{}", RPC_HOST, RPC_PORT, wallet_name);
        let bitcoind = Client::new(&url, auth.clone())?;

        let address = bitcoind.get_new_address(None, None)?;
        bitcoind.generate_to_address(101, &address.assume_checked())?;

        // Setup app components
        let zmq_factory = BitcoinZmqFactory::new(RPC_HOST.to_string(), ZMQ_PORT);
        let db = Database::new(db_path.to_str().unwrap())?;
        let app = App::new(url, auth, zmq_factory, db.clone(), 2);

        Ok(Self {
            bitcoind,
            db,
            app,
            db_path,
        })
    }
}

// #[tokio::test]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mempool_tx_lifecycle() -> Result<()> {
    let mut ctx = TestContext::setup().await?;

    // Initialize app
    ctx.app.init()?;

    // Start app in background
    let app_handle = tokio::spawn(async move {
        ctx.app.run().await.unwrap();
    });

    // Wait for app to initialize and establish ZMQ connection
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create a transaction
    let address = ctx.bitcoind.get_new_address(None, None)?.assume_checked();
    let amount = Amount::from_sat(50_000);
    let txid = ctx
        .bitcoind
        .send_to_address(&address, amount, None, None, None, None, None, None)?;

    // Get raw transaction to verify details
    let tx = ctx.bitcoind.get_raw_transaction(&txid, None)?;
    let computed_txid = tx.compute_txid();

    // Wait a bit for the app to process the mempool tx
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify tx is in database as unconfirmed
    let tx_entry = ctx.db.get_tx_by_txid(&computed_txid)?;
    assert!(tx_entry.is_some());
    // Mine the transaction
    ctx.bitcoind.generate_to_address(1, &address)?;

    // Wait for the app to process the mined tx
    // tokio::time::sleep(Duration::from_secs(2)).await;

    // // Verify tx is marked as mined in database
    // let tx_entry = ctx.db.get_tx_by_txid(&tx)?;
    // assert!(tx_entry.is_some());
    // assert!(tx_entry.unwrap().is_mined);

    // Shutdown app
    app_handle.abort();

    Ok(())
}

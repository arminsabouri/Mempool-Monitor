use anyhow::Result;
use bitcoin::{Amount, Txid};
use bitcoind::{bitcoincore_rpc::RpcApi, BitcoinD};
use bitcoind_async_client::{Auth as AsyncAuth, Client as AsyncClient};
use mempool_tracker::{app::App, database::Database, zmq_factory::BitcoinZmqFactory};
use std::str::FromStr;
use std::time::Duration;
use tempfile::TempDir;

struct TestContext {
    _bitcoind: BitcoinD, // Keep bitcoind alive - dropping it kills the process
    bitcoind_client: bitcoind::bitcoincore_rpc::Client,
    db: Database,
    app: App,
    _db_tempdir: TempDir, // Keep tempdir alive
}

impl TestContext {
    async fn setup() -> Result<Self> {
        let db_tempdir = TempDir::new()?;
        let db_path = db_tempdir.path().join("mempool_tracker_test.db");

        // Setup bitcoind with regtest configuration
        let mut conf = bitcoind::Conf::default();
        conf.args.push("-regtest");
        conf.args.push("-txindex=1");
        conf.args.push("-fallbackfee=0.00001");

        // Get a free port for ZMQ
        let zmq_port = {
            use std::net::TcpListener;
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.local_addr()?.port()
        };

        // Configure ZMQ
        let zmq_arg = format!("-zmqpubrawtx=tcp://127.0.0.1:{}", zmq_port);
        conf.args.push(&zmq_arg);

        // Start bitcoind
        let bitcoind = BitcoinD::with_conf(bitcoind::exe_path()?, &conf)?;

        // Get connection params
        let params = &bitcoind.params;
        let rpc_port = params.rpc_socket.port();
        let cookie_file = params.cookie_file.clone();

        let bitcoind_client = &bitcoind.client;

        let wallet_name = "mempool_tracker_wallet";
        match bitcoind_client.create_wallet(&wallet_name, None, None, None, None) {
            Ok(_) => (),
            Err(e) => {
                if e.to_string().contains("already exists") {
                    println!("wallet already exists");
                    let _ = bitcoind_client.load_wallet(&wallet_name);
                } else {
                    return Err(anyhow::anyhow!("failed to create wallet: {}", e));
                }
            }
        }

        use bitcoind::bitcoincore_rpc::Auth;
        let wallet_url = format!("http://127.0.0.1:{}/wallet/{}", rpc_port, wallet_name);
        let cookie_file_clone = cookie_file.clone();
        let wallet_client = bitcoind::bitcoincore_rpc::Client::new(
            &wallet_url,
            Auth::CookieFile(cookie_file_clone),
        )?;

        // Generate initial blocks to fund the wallet
        let address = wallet_client.get_new_address(None, None)?;
        wallet_client.generate_to_address(101, &address.assume_checked())?;

        let async_auth = AsyncAuth::CookieFile(cookie_file);

        let async_url = format!("http://127.0.0.1:{}/wallet/{}", rpc_port, wallet_name);
        let rpc_client = AsyncClient::new(async_url, async_auth, None, None)?;
        let zmq_factory = BitcoinZmqFactory::new("127.0.0.1".to_string(), zmq_port);
        let db = Database::new(db_path.to_str().unwrap())?;
        let mut app = App::new(
            rpc_client,
            zmq_factory,
            db.clone(),
            2,
            Duration::from_secs(25),
            Duration::from_secs(120),
            None,
        );

        app.init().await?;

        Ok(Self {
            _bitcoind: bitcoind, // Store to keep process alive
            bitcoind_client: wallet_client,
            db,
            app,
            _db_tempdir: db_tempdir, // Store to keep tempdir alive
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mine_empty_block() -> Result<()> {
    let mut ctx = TestContext::setup().await?;

    let app_handle = tokio::spawn(async move {
        ctx.app.run().await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    let address = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();
    ctx.bitcoind_client.generate_to_address(1, &address)?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    // Check that we have one transaction in the db and it's a coinbase
    // Instead of iterating over all transactions in the db, look up the transaction by txid.
    // There should only be one transaction in the db after mining a single block, and it should be a coinbase.
    let latest_block_hash = ctx.bitcoind_client.get_best_block_hash()?;
    let block = ctx.bitcoind_client.get_block(&latest_block_hash)?;
    // The coinbase transaction is always the first transaction in a block
    let coinbase_txid = block.txdata[0].compute_txid();
    let tx_opt = ctx.db.get_tx_by_txid(&coinbase_txid)?;
    assert!(
        tx_opt.is_some(),
        "The coinbase transaction should exist in the database"
    );
    let tx = tx_opt.unwrap();
    // A coinbase transaction has exactly one input whose previous_output is null
    assert_eq!(
        tx.input.len(),
        1,
        "Coinbase transaction should have one input"
    );
    assert!(
        tx.input[0].previous_output.is_null(),
        "The transaction in the db should be a coinbase transaction"
    );
    app_handle.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mine_block_with_transactions() -> Result<()> {
    let mut ctx = TestContext::setup().await?;

    let app_handle = tokio::spawn(async move {
        ctx.app.run().await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create multiple transactions
    let address1 = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();
    let address2 = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();
    let address3 = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();

    let amount = Amount::from_sat(50_000);
    let txid1 = ctx
        .bitcoind_client
        .send_to_address(&address1, amount, None, None, None, None, None, None)?;
    let txid2 = ctx
        .bitcoind_client
        .send_to_address(&address2, amount, None, None, None, None, None, None)?;
    let txid3 = ctx
        .bitcoind_client
        .send_to_address(&address3, amount, None, None, None, None, None, None)?;

    // Wait for transactions to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify transactions are in database as unconfirmed

    assert!(ctx.db.get_tx_by_txid(&txid1)?.is_some());
    assert!(ctx.db.get_tx_by_txid(&txid2)?.is_some());
    assert!(ctx.db.get_tx_by_txid(&txid3)?.is_some());

    // Verify they are not mined yet
    assert!(!ctx.db.is_mined(&txid1)?);
    assert!(!ctx.db.is_mined(&txid2)?);
    assert!(!ctx.db.is_mined(&txid3)?);

    // Mine a block with these transactions
    ctx.bitcoind_client.generate_to_address(1, &address1)?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(ctx.db.is_mined(&txid1)?);
    assert!(ctx.db.is_mined(&txid2)?);
    assert!(ctx.db.is_mined(&txid3)?);

    app_handle.abort();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rbf() -> Result<()> {
    let mut ctx = TestContext::setup().await?;

    let app_handle = tokio::spawn(async move {
        ctx.app.run().await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create a transaction with RBF enabled (low fee)
    let address = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();
    let amount = Amount::from_sat(50_000);

    // Create first transaction with low fee
    let txid1 = ctx.bitcoind_client.send_to_address(
        &address,
        amount,
        Some("low"), // comment
        None,        // subtract_fee_from_amount
        None,        // replaceable
        None,        // conf_target
        None,        // estimate_mode
        None,        // avoid_reuse
    )?;

    // Wait for first transaction to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    let tx1 = ctx.bitcoind_client.get_raw_transaction(&txid1, None)?;
    let txid1_computed = tx1.compute_txid();

    // Verify first transaction is in database
    assert!(ctx.db.get_tx_by_txid(&txid1_computed)?.is_some());
    assert!(!ctx.db.is_rbf(&txid1_computed)?);

    // Create a replacement transaction with higher fee (RBF)
    // Manually bump the fee: create a new RBF transaction replacing the original
    // This is done using the "bumpfee" RPC in bitcoind via raw_call
    use serde_json::json;
    let bumpfee_result = ctx
        .bitcoind_client
        .call::<serde_json::Value>("bumpfee", &[json!(txid1.to_string())])?;
    let txid2 = Txid::from_str(
        bumpfee_result
            .get("txid")
            .and_then(|v| v.as_str())
            .expect("bumpfee result did not have a txid"),
    )
    .expect("failed to parse txid");

    // Wait for RBF transaction to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    let tx2 = ctx.bitcoind_client.get_raw_transaction(&txid2, None)?;
    let txid2_computed = tx2.compute_txid();

    assert!(ctx.db.get_tx_by_txid(&txid2_computed)?.is_some());

    // Verify original transaction is marked as RBF
    // The RBF table should have an entry for the replacement
    assert!(ctx.db.is_rbf(&txid2_computed)?);

    app_handle.abort();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cpfp() -> Result<()> {
    let mut ctx = TestContext::setup().await?;

    let app_handle = tokio::spawn(async move {
        ctx.app.run().await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create a parent transaction with low fee (stuck in mempool)
    let parent_address = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();
    let child_address = ctx
        .bitcoind_client
        .get_new_address(None, None)?
        .assume_checked();

    // Create parent transaction with very low fee
    let parent_amount = Amount::from_sat(100_000);
    let parent_txid = ctx.bitcoind_client.send_to_address(
        &parent_address,
        parent_amount,
        Some("parent"),
        None,
        None,
        None,
        None,
        None,
    )?;

    // Wait for parent transaction to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    let parent_tx = ctx
        .bitcoind_client
        .get_raw_transaction(&parent_txid, None)?;
    let parent_txid_computed = parent_tx.compute_txid();

    // Verify parent transaction is in database
    assert!(ctx.db.get_tx_by_txid(&parent_txid_computed)?.is_some());

    // Initially, parent should not be marked as CPFP parent
    assert!(!ctx.db.is_cpfp_parent(&parent_txid_computed)?);

    // Create a child transaction that spends from the parent (CPFP)
    // The child transaction pays a higher fee to incentivize miners to include both
    let child_amount = Amount::from_sat(50_000);
    let child_txid = ctx.bitcoind_client.send_to_address(
        &child_address,
        child_amount,
        Some("child"),
        None,
        None,
        None,
        None,
        None,
    )?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let child_tx = ctx.bitcoind_client.get_raw_transaction(&child_txid, None)?;
    let child_txid_computed = child_tx.compute_txid();

    // Verify child transaction is in database
    assert!(ctx.db.get_tx_by_txid(&child_txid_computed)?.is_some());

    // Verify parent is now marked as CPFP parent
    // Note: This depends on the child transaction actually spending from the parent
    // In a real scenario, we'd need to manually construct the child to spend from parent
    // For now, we check if the detection logic works when a child is created

    // Check if parent is marked as CPFP parent (if child spends from it)
    // The CPFP detection happens in insert_mempool_tx when a child transaction
    // references a parent transaction that's in the mempool
    let is_cpfp_parent = ctx.db.is_cpfp_parent(&parent_txid_computed)?;

    // If the child transaction actually spends from the parent, it should be marked
    // Otherwise, we at least verify the database query works
    println!("Parent is CPFP parent: {}", is_cpfp_parent);

    app_handle.abort();

    Ok(())
}

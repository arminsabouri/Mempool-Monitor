#[cfg(test)]
mod tests {

    use anyhow::Result;
    use bitcoin::consensus::Decodable;
    use bitcoin::{Amount, Transaction, Txid};
    use bitcoind_async_client::{Auth as AsyncAuth, Client as AsyncClient};
    use corepc_node::{Client, Node, WalletCreateFundedPsbtInput};
    use mempool_tracker::{app::App, database::Database, zmq_factory::BitcoinZmqFactory};
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::time::Duration;
    use tempfile::TempDir;

    struct TestContext {
        _node: Node,        // Keep node alive - dropping it kills the process
        rpc_client: Client, // RPC client connected to the wallet
        db: Database,
        app: App,
        _db_tempdir: TempDir, // Keep tempdir alive
    }

    impl TestContext {
        async fn setup() -> Result<Self> {
            let db_tempdir = TempDir::new()?;
            let db_path = db_tempdir.path().join("mempool_tracker_test.db");

            // Setup node with regtest configuration
            let mut conf = corepc_node::Conf::default();
            conf.args.push("-txindex=1");
            conf.args.push("-fallbackfee=0.00001");
            conf.enable_zmq = true; // Enable ZMQ for raw transaction publishing
            conf.wallet = Some("mempool_tracker_wallet".to_string());

            // Start node using downloaded binary
            let node = Node::from_downloaded_with_conf(&conf)?;

            // Get connection params
            let params = &node.params;
            let rpc_port = params.rpc_socket.port();
            let cookie_file = params.cookie_file.clone();
            let zmq_port = params
                .zmq_pub_raw_tx_socket
                .map(|s| s.port())
                .ok_or_else(|| anyhow::anyhow!("ZMQ socket not available"))?;

            // Create a new client connected to the wallet
            // The node already has the wallet loaded (via conf.wallet)
            use corepc_node::client::client_sync::Auth;
            let wallet_name = "mempool_tracker_wallet";
            let wallet_url = format!("http://127.0.0.1:{}/wallet/{}", rpc_port, wallet_name);
            let rpc_client =
                Client::new_with_auth(&wallet_url, Auth::CookieFile(cookie_file.clone()))?;

            // Generate initial blocks to fund the wallet
            let address = rpc_client.new_address()?;
            rpc_client.generate_to_address(101, &address)?;

            // Setup async client for the app
            let async_auth = AsyncAuth::CookieFile(cookie_file);
            let wallet_name = "mempool_tracker_wallet";
            let async_url = format!("http://127.0.0.1:{}/wallet/{}", rpc_port, wallet_name);
            let zmq_factory = BitcoinZmqFactory::new("127.0.0.1".to_string(), zmq_port);
            let db = Database::new(db_path.to_str().unwrap())?;
            let mut app = App::new(
                AsyncClient::new(async_url, async_auth, None, None)?,
                zmq_factory,
                db.clone(),
                2,
                Duration::from_secs(25),
                Duration::from_secs(120),
                false, // disable_prune_check
                None,
            );

            app.init().await?;

            Ok(Self {
                _node: node, // Store to keep process alive
                rpc_client,
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
        let address = ctx.rpc_client.new_address()?;
        ctx.rpc_client.generate_to_address(1, &address)?;

        tokio::time::sleep(Duration::from_secs(2)).await;
        // Check that we have one transaction in the db and it's a coinbase
        // Instead of iterating over all transactions in the db, look up the transaction by txid.
        // There should only be one transaction in the db after mining a single block, and it should be a coinbase.
        let latest_block_hash = ctx.rpc_client.best_block_hash()?;
        let block = ctx.rpc_client.get_block(latest_block_hash)?;
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
        let address1 = ctx.rpc_client.new_address()?;
        let address2 = ctx.rpc_client.new_address()?;
        let address3 = ctx.rpc_client.new_address()?;

        let amount = Amount::from_sat(50_000);
        let txid1 = ctx.rpc_client.send_to_address(&address1, amount)?.txid()?;
        let txid2 = ctx.rpc_client.send_to_address(&address2, amount)?.txid()?;
        let txid3 = ctx.rpc_client.send_to_address(&address3, amount)?.txid()?;

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
        ctx.rpc_client.generate_to_address(1, &address1)?;

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
        let address = ctx.rpc_client.new_address()?;
        let amount = Amount::from_sat(50_000);

        // Create first transaction with low fee
        let txid1 = ctx.rpc_client.send_to_address(&address, amount)?.txid()?;

        // Wait for first transaction to be processed
        tokio::time::sleep(Duration::from_secs(5)).await;

        let tx1 = ctx.rpc_client.get_raw_transaction(txid1)?.transaction()?;
        let txid1_computed = tx1.compute_txid();

        // Verify first transaction is in database
        assert!(ctx.db.get_tx_by_txid(&txid1_computed)?.is_some());
        assert!(!ctx.db.is_rbf(&txid1_computed)?);

        // Create a replacement transaction with higher fee (RBF)
        // Manually bump the fee: create a new RBF transaction replacing the original
        // This is done using the "bumpfee" RPC in bitcoind via raw_call
        use serde_json::json;
        let bumpfee_result = ctx
            .rpc_client
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

        let tx2 = ctx.rpc_client.get_raw_transaction(txid2)?.transaction()?;
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

        let parent_txid = ctx
            .rpc_client
            .send_to_address(&ctx.rpc_client.new_address()?, Amount::from_sat(100_000))?
            .txid()?;

        // Wait for parent transaction to be processed
        tokio::time::sleep(Duration::from_secs(5)).await;

        let parent_tx = ctx
            .rpc_client
            .get_raw_transaction(parent_txid)?
            .transaction()?;

        let parent_txid = parent_tx.compute_txid();

        // Verify parent transaction is in database
        assert!(ctx.db.get_tx_by_txid(&parent_txid)?.is_some());

        // Initially, parent should not be marked as CPFP parent
        assert!(ctx.db.child_txid(&parent_txid)?.is_none());

        // Create and sign the child tx
        let mut outputs = BTreeMap::new();
        outputs.insert(ctx.rpc_client.new_address()?, Amount::from_sat(90_000));

        let psbt = ctx
            .rpc_client
            .wallet_create_funded_psbt(
                vec![WalletCreateFundedPsbtInput::new(parent_txid, 0)],
                vec![outputs],
            )?
            .psbt;
        let signed_psbt = ctx
            .rpc_client
            .wallet_process_psbt(&bitcoin::Psbt::from_str(&psbt)?)?;
        let hex = hex::decode(signed_psbt.hex.unwrap())?;
        let child_tx = Transaction::consensus_decode(&mut hex.as_slice())?;
        let child_txid = ctx.rpc_client.send_raw_transaction(&child_tx)?.txid()?;
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify child transaction is in database
        assert!(ctx.db.get_tx_by_txid(&child_txid)?.is_some());

        let db_child_txid = ctx.db.child_txid(&parent_txid)?.unwrap();
        assert_eq!(db_child_txid, child_txid);

        let db_parent_txid = ctx.db.parent_txid(&child_txid)?.unwrap();
        assert_eq!(db_parent_txid, parent_txid);
        app_handle.abort();

        Ok(())
    }
}

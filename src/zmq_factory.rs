use anyhow::Result;
use bitcoincore_zmq::MessageStream;

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

# Mempool Monitor

A Rust-based Bitcoin mempool monitoring tool that tracks transactions, detects Replace-By-Fee (RBF) updates, and when transactions are mined. This tool connects directly to your Bitcoin node via ZMQ and RPC to provide real-time mempool analysis.
The goal of this project is to collect data on unconfirmed transactions to help with various data driven research projects.

## Prerequisites

- Rust toolchain (1.70 or later)
- Running Bitcoin Core node with:
  - txindex=1
  - RPC enabled (authentication via user and password)
  - ZMQ enabled (zmqpubrawtx=tcp://127.0.0.1:28332)

## Usage

Example regtest run:

```bash
cargo run -- --bitcoind-user foo --bitcoind-password bar --bitcoind-host "127.0.0.1" --bitcoind-rpc-port 18443 --bitcoind-zmq-port 28373
```

## TODO

- [ ] Replace sled with something that allows you to query via versious indecies
- [ ] multi-threading
- [ ] add tests
- [ ] add record CPFP

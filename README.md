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

## Building

```bash
docker run -it --rm -v "$PWD":/volume messense/rust-musl-cross:x86_64-musl
cd /volume
cargo build --release --target x86_64-unknown-linux-musl
```

## TODO

- [X] Replace sled with something that allows you to query via versious indecies
- [X] multi-threading
- [ ] add tests
- [X] add record CPFP
- [X] Track prune
- [ ] Same inputs being spent in other tx
- [X] prune large witnesses
- [X] Capture mempool size and tx count at the time of entry and exit
- [ ] Some CI checks: Lint, clippy, future tests

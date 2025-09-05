#!/bin/env bash

docker run --rm -it -v "$PWD":/volume -w /volume messense/rust-musl-cross:x86_64-musl cargo build --release --target x86_64-unknown-linux-musl

#!/usr/bin/env bash

set -e
for i in {1..1000}; do
    echo ${i}
    RUSTFLAGS="-Z sanitizer=address" cargo test --release --target x86_64-unknown-linux-gnu -- --nocapture
done
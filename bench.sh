#!/usr/bin/env bash
set -euo pipefail

cargo build --release
cargo bench --bench throughput -- "$@"

#!/usr/bin/env bash
# Debug build against the databases of e2e/docker-compose.yml (PostgreSQL -> MongoDB).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf ./dbcTemp
cargo build
./target/debug/rustygrator -f "postgresql://user:password@127.0.0.1:5433/test?schema=public" -t "mongodb://root:example@127.0.0.1:27019/test?authSource=admin"

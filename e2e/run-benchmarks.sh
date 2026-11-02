#!/usr/bin/env bash
# Runs the end-to-end benchmarks (default: quick profile, a few minutes).
#   e2e/run-benchmarks.sh [quick|full] [--save-baseline]
# Micro-benchmarks: cargo bench -p rustygrator-core
set -euo pipefail
cd "$(dirname "$0")"
export BENCH_PROFILE="${1:-quick}"
if [[ "${2:-}" == "--save-baseline" ]]; then
    export BENCH_SAVE_BASELINE=1
fi
mkdir -p bench-results
status=0
docker compose -f docker-compose.bench.yml up --build --abort-on-container-exit --exit-code-from bench || status=$?
docker compose -f docker-compose.bench.yml down -v
exit "$status"

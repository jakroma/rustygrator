#!/usr/bin/env bash
# Runs the whole test suite against PostgreSQL, MongoDB, ScyllaDB, MySQL, Oracle and SQLite in
# docker-compose (the tests run inside the `tests` container).
#   e2e/run-tests.sh
set -euo pipefail
cd "$(dirname "$0")"
status=0
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests || status=$?
docker compose -f docker-compose.test.yml down -v
exit "$status"

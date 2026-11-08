#!/usr/bin/env bash
# Linux/macOS (and the AWS EC2 demo host) version of run-benchmarks.ps1.
#   ./scripts/run-benchmarks.sh [--rows 1000000] [--with-bridge] [--where "AWS EC2 m7i-flex.large"]
set -euo pipefail
cd "$(dirname "$0")/.."
ROWS=1000000; VUS=20; DURATION=60s; WITH_BRIDGE=0; WHERE="local Docker"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows) ROWS="$2"; shift 2;;
    --vus) VUS="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --with-bridge) WITH_BRIDGE=1; shift;;
    --where) WHERE="$2"; shift 2;;
    *) echo "unknown arg $1"; exit 2;;
  esac
done
export BENCH_WHERE="$WHERE"
COMPOSE=${COMPOSE:-docker compose}
T="$COMPOSE --profile tools"
mkdir -p load-tests/results
step() { echo; echo "=== $1 ==="; }

step "Prepare $ROWS flights";            $T run --rm bench dist/scripts/bench-prep.js --rows "$ROWS" --index on
step "EXPLAIN ANALYZE";                  $T run --rm bench dist/scripts/explain-benchmark.js --runs 50
step "Drop the index";                   $T run --rm bench dist/scripts/bench-prep.js --index off
step "k6 search, NO index";              $T run --rm k6 run -q -e MODE=nocache -e LABEL=search-noindex -e VUS="$VUS" -e DURATION="$DURATION" /scripts/search.js
step "Recreate the index";               $T run --rm bench dist/scripts/bench-prep.js --index on
step "k6 search, index";                 $T run --rm k6 run -q -e MODE=nocache -e LABEL=search-index -e VUS="$VUS" -e DURATION="$DURATION" /scripts/search.js
step "k6 search, index + cache";         $T run --rm k6 run -q -e MODE=cache -e LABEL=search-index-cache -e VUS="$VUS" -e DURATION="$DURATION" /scripts/search.js
step "k6 idempotency race";              $T run --rm k6 run -q -e VUS=100 /scripts/idempotency-race.js

step "10x booking spike + consumer-lag monitor"
docker rm -f skybook-spike-monitor >/dev/null 2>&1 || true
$T run -d --name skybook-spike-monitor bench dist/scripts/bench-spike.js --max 900 >/dev/null
sleep 3
$T run --rm k6 run -q /scripts/booking-burst.js || true
echo "waiting for consumers to catch up..."
docker wait skybook-spike-monitor >/dev/null; docker logs skybook-spike-monitor; docker rm skybook-spike-monitor >/dev/null

if [[ "$WITH_BRIDGE" == 1 ]]; then
  step "Bridge outage test"
  $COMPOSE --profile aws up -d fake-sqs aws-bridge; sleep 15
  $COMPOSE --profile aws --profile tools run --rm bridge-outage-test
fi

step "Write RESULTS.md";                 $T run --rm bench dist/scripts/bench-report.js
echo; echo "Done: load-tests/results/RESULTS.md"

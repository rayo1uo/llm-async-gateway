#!/usr/bin/env bash
# End-to-end demo: one nearline request and one batch against the mock upstream.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GATEWAY_PORT="${GATEWAY_PORT:-18080}"
MOCK_PORT="${MOCK_PORT:-18090}"
REDIS_PORT="${REDIS_PORT:-6379}"
BASE="http://127.0.0.1:${GATEWAY_PORT}"

if ! command -v redis-cli >/dev/null || ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  echo "starting redis on :${REDIS_PORT}"
  redis-server --daemonize yes --bind 127.0.0.1 --port "$REDIS_PORT" --dir /tmp --save "" --appendonly no --dbfilename "demo-${REDIS_PORT}.rdb"
  for _ in $(seq 1 50); do
    redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1 && break
    sleep 0.1
  done
fi

mkdir -p bin
if [[ ! -x bin/gateway || ! -x bin/mockupstream ]]; then
  go build -o bin/gateway ./cmd/gateway
  go build -o bin/mockupstream ./cmd/mockupstream
fi

bin/mockupstream -addr "127.0.0.1:${MOCK_PORT}" -delay 30ms > /tmp/llm-async-mock.log 2>&1 &
MOCK_PID=$!
GATEWAY_ADDR="127.0.0.1:${GATEWAY_PORT}" \
  REDIS_ADDR="127.0.0.1:${REDIS_PORT}" \
  UPSTREAM_URL="http://127.0.0.1:${MOCK_PORT}" \
  KEY_PREFIX="demo${GATEWAY_PORT}" \
  POLL_INTERVAL=20ms \
  LOG_LEVEL=info \
  bin/gateway > /tmp/llm-async-gateway.log 2>&1 &
GW_PID=$!
cleanup() {
  kill "$GW_PID" "$MOCK_PID" >/dev/null 2>&1 || true
  wait "$GW_PID" "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  curl -sf "$BASE/readyz" >/dev/null && break
  sleep 0.1
done
curl -sf "$BASE/readyz" >/dev/null

echo "== nearline =="
REQ=$(curl -sf -X POST "$BASE/v1/requests" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-nearline' \
  -d '{"endpoint":"/v1/chat/completions","deadline_seconds":60,"metadata":{"tenant":"demo"},"body":{"model":"mock","messages":[{"role":"user","content":"hello from nearline"}]}}')
echo "$REQ"
REQ_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$REQ")
NEAR=""
for _ in $(seq 1 50); do
  NEAR=$(curl -sf "$BASE/v1/requests/$REQ_ID")
  STATUS=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$NEAR")
  if [[ "$STATUS" != "queued" && "$STATUS" != "in_progress" && "$STATUS" != "cancelling" ]]; then
    break
  fi
  sleep 0.1
done
echo "$NEAR"

echo "== batch =="
FILE=$(curl -sf -F purpose=batch -F "file=@examples/batch_input.jsonl" "$BASE/v1/files")
echo "$FILE"
FILE_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$FILE")
BATCH=$(curl -sf -X POST "$BASE/v1/batches" \
  -H 'Content-Type: application/json' \
  -d "{\"input_file_id\":\"$FILE_ID\",\"endpoint\":\"/v1/chat/completions\",\"completion_window\":\"1h\",\"metadata\":{\"job\":\"demo\"}}")
echo "$BATCH"
BATCH_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$BATCH")
FINAL=""
for _ in $(seq 1 50); do
  FINAL=$(curl -sf "$BASE/v1/batches/$BATCH_ID")
  STATUS=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$FINAL")
  if [[ "$STATUS" != "validating" && "$STATUS" != "in_progress" && "$STATUS" != "finalizing" && "$STATUS" != "cancelling" ]]; then
    break
  fi
  sleep 0.1
done
echo "$FINAL"
OUT_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["output_file_id"])' <<<"$FINAL")
echo "== batch output =="
curl -sf "$BASE/v1/files/$OUT_ID/content"
echo
echo "demo ok"

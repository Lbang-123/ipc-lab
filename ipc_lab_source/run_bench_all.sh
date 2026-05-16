#!/usr/bin/env bash
set -euo pipefail
make
for m in fifo unix msgq shm; do
  echo "===== $m ====="
  rm -f /tmp/ipc_fifo_c2s /tmp/ipc_fifo_s2c /tmp/ipc_unix_stream.sock || true
  ipcrm -Q 0x12345 2>/dev/null || true
  ipcrm -M 0x23456 2>/dev/null || true
  ipcrm -S 0x23457 2>/dev/null || true
  ./ipc_lab server "$m" > "server_${m}.log" 2>&1 &
  spid=$!
  sleep 0.3
  if [ "$m" = "msgq" ]; then
    ./ipc_lab client "$m"
  else
    ./ipc_lab client "$m" "hello from client"
  fi
  ./ipc_lab bench "$m" | tee "bench_${m}.txt"
  wait "$spid" || true
  cat "server_${m}.log"
done

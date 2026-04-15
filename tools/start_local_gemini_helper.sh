#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/runtime"
LOG_FILE="$LOG_DIR/local_gemini_helper.log"
PID_FILE="$LOG_DIR/local_gemini_helper.pid"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "local gemini helper already running with pid $(cat "$PID_FILE")"
  exit 0
fi

nohup python3 "$ROOT_DIR/tools/local_gemini_helper.py" >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
echo "started local gemini helper, pid=$(cat "$PID_FILE"), log=$LOG_FILE"

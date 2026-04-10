#!/usr/bin/env bash
set -euo pipefail
cd /home/blank/playground/prefect_demo
exec python3 -u tools/local_codex_helper.py

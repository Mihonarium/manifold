#!/usr/bin/env bash
set -euo pipefail
CORES="${CORES:-0,1}"   # pin harness to 2 of 16 cores; leaves >=12 free
exec taskset -c "$CORES" node harness.js "$@"

#!/usr/bin/env bash
# Serial push (scatter) sweep -- the single-thread baseline matching the
# parallel pull sweeper's scale and degree grid.
#
#   V        = 61,578,415  (GAP-twitter vertex count)
#   kernel   = push (CSR scatter: vprop[col_idx]++)
#   thread   = 1, pinned to core 0 (NUMA node 0)
#   degree   = 1,2,4,8,16,24,32
#
# Same degree cap as the pull sweeper: edge count V*degree must stay under 4B
# because row_ptr/col_ptr are uint32_t. At V=61.5M, k=64 -> 3.94B edges (< 4B,
# the top point); k=128 -> 7.9B would overflow. Wrapped in numactl --membind=0
# so vprop is local to the pinned core's node. 
set -euo pipefail
cd "$(dirname "$0")"

make driver >/dev/null

V=61578415
RUNS=5
SEED=42
OUT="${1:-results/csr_push_twitter_scale.csv}"

KS=(-k 1 -k 2 -k 4 -k 8 -k 16 -k 24 -k 32 -k 64)

NUMACTL=()
if command -v numactl >/dev/null 2>&1; then
    NUMACTL=(numactl --cpunodebind=0 --membind=0)
else
    echo "warning: numactl not found; vprop may land off-node (noisier)" >&2
fi

mkdir -p "$(dirname "$OUT")"
"${NUMACTL[@]}" ./driver -V "$V" -kernel push -c 0 \
    "${KS[@]}" -r "$RUNS" -s "$SEED" -o "$OUT"
echo "wrote $OUT"

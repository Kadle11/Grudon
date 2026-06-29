#!/usr/bin/env bash
# Parallel pull (gather) sweep at GAP-twitter scale.
#
#   V        = 61,578,415  (GAP-twitter vertex count)
#   kernel   = pull (CSC gather: out[v] = sum vprop[in-neighbours])
#   threads  = 16, pinned to physical cores 0,2,...,30
#   degree   = 1,2,4,8,16,24,32
#
# Degree cap: edge count V*degree must stay under 4B because row_ptr/col_ptr
# are uint32_t. At V=61.5M, k=64 -> 3.94B edges (< 4B, the top point); k=128 ->
# 7.9B would overflow. The high-degree points are large: k=64 transiently
# allocates ~63 GB of edge buffers (the driver's
# memory guard skips any point that won't fit).
set -euo pipefail
cd "$(dirname "$0")"

V=61578415
THREADS=16
RUNS=5
SEED=42

usage() {
    cat <<EOF
usage: $(basename "$0") [-p|--pin] [-o|--output FILE]

  -p, --pin           pin ${THREADS} threads to physical cores 0,2,...,30
  -o, --output FILE   write results to FILE
                      (default: results/csc_pull_twitter_scale_t_${THREADS}.csv)
  -h, --help          show this help
EOF
}

PIN=0
OUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p | --pin) PIN=1 ;;
        -o | --output)
            OUT="$2"
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) OUT="$1" ;;
    esac
    shift
done
OUT="${OUT:-results/csc_pull_twitter_scale_t_${THREADS}.csv}"

make driver >/dev/null

# 16 physical cores on NUMA node 0: 0,2,4,...,30
CPUS=()
if ((PIN)); then
    for ((c = 0; c < 32; c += 2)); do CPUS+=(-c "$c"); done
fi

KS=(-k 1 -k 2 -k 4 -k 8 -k 16 -k 24 -k 32)

NUMACTL=()
if command -v numactl >/dev/null 2>&1; then
    NUMACTL=(numactl --cpunodebind=0 --membind=0)
else
    echo "warning: numactl not found; vprop/out may land off-node (noisier)" >&2
fi

mkdir -p "$(dirname "$OUT")"
"${NUMACTL[@]}" ./driver -V "$V" -kernel pull -t "$THREADS" "${CPUS[@]}" \
    "${KS[@]}" -r "$RUNS" -s "$SEED" -o "$OUT"
echo "wrote $OUT"

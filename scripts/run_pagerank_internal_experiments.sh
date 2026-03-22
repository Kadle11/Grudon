#!/bin/bash

set -euo pipefail

BASE_CMD_DEFAULT="mpirun -n 2 --npersocket 1 --map-by NUMA:PE=2 --use-hwthread-cpus --report-bindings ./build/bin/Debug/Grudon -g graphs/galois/sgr/soc-LiveJournal1.mtx.sgr -c 1 -m 1 -t 4 -p partitions/soc-LiveJournal1.mtx.4parts"
BASE_CMD="${GRUDON_BASE_CMD:-$BASE_CMD_DEFAULT}"
EVENTS="${GRUDON_PR_INTERNAL_EVENTS:-cycles,instructions}"
RUN_COUNT="${GRUDON_PR_INTERNAL_RUN_COUNT:-3}"
OUTPUT_ROOT="${GRUDON_PR_INTERNAL_OUTPUT_ROOT:-output/pr_internal}"

mkdir -p "$OUTPUT_ROOT"

export GRUDON_ENABLE_PERF_PROFILE=1
export GRUDON_ENABLE_PR_INTERNAL_PROFILE=1
export GRUDON_PERF_EVENTS="$EVENTS"

echo "Starting PageRank internal profiling experiments"
echo "Base command: $BASE_CMD"
echo "Events: $GRUDON_PERF_EVENTS"
echo "Runs: $RUN_COUNT"
echo "Output root: $OUTPUT_ROOT"

for ((i = 0; i < RUN_COUNT; i++)); do
  run_dir="$OUTPUT_ROOT/run_${i}"
  mkdir -p "$run_dir"

  export GRUDON_PROFILE_PREFIX="pagerank_internal_run_${i}"
  export GRUDON_PROFILE_OUTPUT_DIR="$run_dir"

  echo "=========================================================="
  echo "Run $((i + 1))/$RUN_COUNT"
  echo "Output Prefix: $GRUDON_PROFILE_PREFIX"
  echo "=========================================================="

  eval "$BASE_CMD" | tee "$run_dir/grudon_output.txt"

  python3 scripts/summarize_pagerank_internal_cycles.py \
    --input-pattern "$run_dir/*.json" \
    --output-json "$run_dir/pagerank_internal_top3_cycles.json" \
    --output-markdown "$run_dir/pagerank_internal_top3_cycles.md"

done

echo "All runs complete."
echo "Aggregating across all runs..."
python3 scripts/summarize_pagerank_internal_cycles.py \
  --input-pattern "$OUTPUT_ROOT/run_*/*.json" \
  --output-json "$OUTPUT_ROOT/pagerank_internal_top3_cycles.json" \
  --output-markdown "$OUTPUT_ROOT/pagerank_internal_top3_cycles.md"

echo "Done."

#!/bin/bash

# Define the base command
BASE_CMD="mpirun -n 2 --npersocket 1 --map-by NUMA:PE=2 --use-hwthread-cpus --report-bindings ./build/bin/Debug/Grudon -g graphs/galois/sgr/soc-LiveJournal1.mtx.sgr -c 1 -m 1 -t 4 -p partitions/soc-LiveJournal1.mtx.4parts"
    
# Array of different event combinations to profile
#    "mem_uops_retired.all_loads,mem_load_uops_retired.l3_miss,mem_uops_retired.all_stores"

# "mem_load_uops_retired.l3_miss"
# "instructions,cycles,cache-misses"
declare -a EVENTS_TO_PROFILE=(
    "dTLB-loads"
)

# Create an output directory if it doesn't exist
mkdir -p output

# Ensure perf profiling is enabled
export GRUDON_ENABLE_PERF_PROFILE=1

echo "Starting Grudon Profiling Experiments..."

# Loop through each set of events
for i in "${!EVENTS_TO_PROFILE[@]}"; do
    events="${EVENTS_TO_PROFILE[$i]}"
    
    # We can pass GRUDON_PERF_EVENTS for our updated C++ code
    export GRUDON_PERF_EVENTS="$events"
    
    # The C++ code now automatically appends the abbreviation (e.g., _ic) to the prefix
    export GRUDON_PROFILE_PREFIX="grudon_run_${i}"
    export GRUDON_PROFILE_OUTPUT_DIR="output/run_${i}"
    
    echo "=========================================================="
    echo "Run $(($i + 1))/3: Profiling events -> $events"
    echo "Output Prefix: $GRUDON_PROFILE_PREFIX"
    echo "=========================================================="
    
    # Run Grudon with the given parameters
    $BASE_CMD | tee "output/run_${i}/grudon_output.txt"
    
    echo "Run $(($i + 1)) completed!"
    echo "----------------------------------------------------------"
done

echo "All 3 runs completed successfully. Check the output/ directory for results."

#!/bin/bash

export PATH=/usr/bin:$PATH

set -x # Print commands
set -e

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation

GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/sensitivity_analysis"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
COMPUTE_NODE_CORES=$2

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)
GRAPH_DIR=$(dirname $GRAPH_PATH)
SYM_GRAPH_PATH=$GRAPH_DIR/$GRAPH_NAME.sgr

PARTITION_PATH=$PARTITIONS_DIR/$GRAPH_NAME.3gparts


mkdir -p $LOG_DIR
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"

# Set default number of cores per node
if [ -z "$COMPUTE_NODE_CORES" ]; then
    COMPUTE_NODE_CORES=4
fi

MEMORY_NODE_LCORES=$(echo "$COMPUTE_NODE_CORES * 2" | awk -F* '{print $1*$2}')
MEMORY_NODE_CORES=$(echo "$MEMORY_NODE_LCORES * 0.5" | awk -F* '{print $1*$2}')


# Check if the graph exists
if [ ! -f $GRAPH_PATH ]; then
    echo "Graph file not found: $GRAPH_PATH"
    exit 1
fi

# Check if the symmetric graph exists
if [ ! -f $SYM_GRAPH_PATH ]; then
    echo "Symmetric graph file not found: $SYM_GRAPH_PATH"
#    exit 1      
fi

# Function to Reset HugePage Allocation 
function reset_hugepage_allocation {
    pkill membw | true

    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 

#    sudo sysctl -w vm.nr_hugepages=0
#    sudo sysctl -w vm.nr_overcommit_hugepages=0

    sleep 5
    
    $EMULATION_SCRIPTS/NUMA_cores.sh
    sleep 5
#    sudo sysctl -w vm.nr_hugepages=102400
}

cd $EMULATION_SCRIPTS
sudo ./disagg_NDP.sh

for x in $(seq -1 0.5 3); do
  for y in $(seq -1.2 0.5 3.2); do
    echo "Building with X=$x and Y=$y"

    # Convert to macro-compatible format (e.g., 0.5 -> 0_5, -1.2 -> -1_2)
    x_macro=$(echo $x | sed 's/-/neg_/; s/\./_/')
    y_macro=$(echo $y | sed 's/-/neg_/; s/\./_/')

    LOG_PREFIX="sensitivity_analysis_OC${x_macro}_FC${y_macro}"

    # Go to the build directory
    cd $GROUDON_HOME/build

    # Clean previous builds
    rm -rf CMakeCache.txt || true

    # Configure with CMake
    cmake .. -DOFFLOAD_COEFFICIENT=$x -DFETCH_COEFFICIENT=$y -DCMAKE_BUILD_TYPE=Debug -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON 

    # Build
    make -j 

    # Back to the run scripts directory
    cd $RUN_SCRIPTS_DIR

    # Check if the partition file exists
    if [ ! -f $PARTITION_PATH ]; then
        for i in {1..3}; 
        do
            LOG_SUFFIX="$LOG_PREFIX"_run$i.log
            reset_hugepage_allocation
            GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a pr -o 0 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
            # reset_hugepage_allocation
            # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a cc  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
            # reset_hugepage_allocation
            # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a sssp 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
        done
    else
        for i in {1..3}; 
        do
            LOG_SUFFIX="$LOG_PREFIX"_run$i.log
            reset_hugepage_allocation
            GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a pr -p $PARTITION_PATH -o 0 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
            # reset_hugepage_allocation
            # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a cc -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
            # reset_hugepage_allocation
            # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a sssp -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
        done
    fi
  done
done
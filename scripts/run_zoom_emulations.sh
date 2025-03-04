#!/bin/bash

export PATH=/usr/bin:$PATH

set -x # Print commands
set -e

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
GROUDON_BIN_PR="$GROUDON_HOME/build/bin/Debug/Groudon_PR"
LOG_DIR="$GROUDON_HOME/logs/inc_overhead_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
COMPUTE_NODE_CORES=$2
CONFIG=$3

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

# Set default configuration
if [ -z "$CONFIG" ]; then
    CONFIG=X
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

#    sudo sysctl -w vm.nr_hugepages=102400

   $EMULATION_SCRIPTS/NUMA_cores.sh

    sleep 5
}

cd $EMULATION_SCRIPTS
sudo ./disagg_NDP.sh

cd $RUN_SCRIPTS_DIR
# Check if the partition file exists
if [ ! -f $PARTITION_PATH ]; then
    for i in {1..1}; 
    do
        LOG_SUFFIX=Config"$CONFIG"_Groudon_"$GRAPH_NAME"_3N_run$i.log 
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN_PR -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a pr --max-iterations 20 -o 1 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a cc -o 1 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a sssp -o 1 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
    done
else
    for i in {1..1}; 
    do
        LOG_SUFFIX=Config"$CONFIG"_Groudon_"$GRAPH_NAME"_3N_run$i.log 
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN_PR -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a pr -p $PARTITION_PATH --max-iterations 20 -o 1 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a cc -p $PARTITION_PATH -o 1 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $MEMORY_NODE_LCORES -a sssp -p $PARTITION_PATH -o 1 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
    done
fi
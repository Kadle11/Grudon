#!/bin/bash

export PATH=/usr/bin:$PATH

set -x # Print commands
set -e

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/deployment_dm_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)
GRAPH_DIR=$(dirname $GRAPH_PATH)
SYM_GRAPH_PATH=$GRAPH_DIR/$GRAPH_NAME.sgr

PARTITION_PATH=$PARTITIONS_DIR/$GRAPH_NAME.3gparts


mkdir -p $LOG_DIR
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"

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

function reset_hugepage_allocation {
    pkill membw | true

    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 

    sleep 5
}

# Check if the partition file exists
if [ ! -f $PARTITION_PATH ]; then
    LOG_SUFFIX=Groudon_"$GRAPH_NAME"_3N.log 
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a pr --max-iterations 20 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t 20 -a cc  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a sssp 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
else
    LOG_SUFFIX=Groudon_"$GRAPH_NAME"_3N.log 
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a pr -p $PARTITION_PATH --max-iterations 20  2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t 20 -a cc -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a sssp -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
fi

if [ ! -f $PARTITION_PATH ]; then
    LOG_SUFFIX=Disaggregated_"$GRAPH_NAME"_3N.log 
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a pr --max-iterations 20 -o 2 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t 20 -a cc -m 2 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a sssp -m 2 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
else
    LOG_SUFFIX=Disaggregated_"$GRAPH_NAME"_3N.log
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a pr -p $PARTITION_PATH --max-iterations 20 -o 2 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t 20 -a cc -p $PARTITION_PATH -m 2 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    # reset_hugepage_allocation
    # GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --map-by socket:PE=20 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t 20 -a sssp -p $PARTITION_PATH -m 2 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
fi


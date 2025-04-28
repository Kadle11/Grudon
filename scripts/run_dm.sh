#!/bin/bash

export PATH=/usr/bin:$PATH

set -x # Print commands
set -e

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"

GROUDON_MINBIN="$GROUDON_HOME/build/bin/Debug/GroudonNDPMin"
GROUDON_ADDBIN="$GROUDON_HOME/build/bin/Debug/GroudonNDPAdd"
GROUDON_ADDAGG="$GROUDON_HOME/build/bin/Debug/GroudonAggAdd"
GROUDON_MINAGG="$GROUDON_HOME/build/bin/Debug/GroudonAggMin"

LOG_DIR="$GROUDON_HOME/logs/agg_dm_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
NUM_PARTITIONS=$2
NUM_PROCS=$(($NUM_PARTITIONS + 1))

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)
GRAPH_DIR=$(dirname $GRAPH_PATH)
SYM_GRAPH_PATH=$GRAPH_DIR/$GRAPH_NAME.sgr

PARTITION_PATH=$PARTITIONS_DIR/$GRAPH_NAME."$NUM_PARTITIONS"gparts


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

# # Disaggregated Runs
# if [ ! -f $PARTITION_PATH ]; then
#     LOG_SUFFIX=Disaggregated_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -o 2 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -o 2 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -o 2 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# else
#     LOG_SUFFIX=Disaggregated_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -p $PARTITION_PATH -o 2 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -p $PARTITION_PATH -o 2 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -p $PARTITION_PATH -o 2 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# fi


# # Naive NDP Only Runs
# if [ ! -f $PARTITION_PATH ]; then
#     LOG_SUFFIX=GroudonNaiveNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -o 0 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -o 0 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -o 0 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# else
#     LOG_SUFFIX=GroudonNaiveNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -p $PARTITION_PATH -o 0 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -p $PARTITION_PATH -o 0 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -p $PARTITION_PATH -o 0 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# fi



# # NDP Engine Only Runs
# if [ ! -f $PARTITION_PATH ]; then
#     LOG_SUFFIX=GroudonNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr  2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp  2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# else
#     LOG_SUFFIX=GroudonNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_ADDBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -p $PARTITION_PATH  2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -p $PARTITION_PATH  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     reset_hugepage_allocation
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=5 --use-hwthread-cpus --report-bindings $GROUDON_MINBIN -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -p $PARTITION_PATH  2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# fi

# Groudon Runs
if [ ! -f $PARTITION_PATH ]; then
    LOG_SUFFIX=Groudon_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_ADDAGG -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr  2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_MINAGG -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_MINAGG -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp  2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
else
    LOG_SUFFIX=Groudon_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_ADDAGG -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a pr -p $PARTITION_PATH   2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_MINAGG -g $SYM_GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a cc -p $PARTITION_PATH  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $NUM_PROCS --map-by socket:PE=10 --use-hwthread-cpus --report-bindings $GROUDON_MINAGG -g $GRAPH_PATH -c 1 -m $NUM_PARTITIONS -t 20 -a sssp -p $PARTITION_PATH  2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
fi

#!/bin/bash

export PATH=/usr/bin:$PATH

set -x # Print commands

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation
GRAPH_PATH=/netscratch/vrao79/galois-graphs/soc-LiveJournal1.gr

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/emulation_runs"
PARTITIONS_DIR="/netscratch/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
CORES_PER_NODE=$2

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)
GRAPH_DIR=$(dirname $GRAPH_PATH)
SYM_GRAPH_PATH=$GRAPH_DIR/../sgr/$GRAPH_NAME.sgr

PARTITION_PATH=$PARTITIONS_DIR/$GRAPH_NAME.3parts


mkdir -p $LOG_DIR
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"

# Set default number of cores per node
if [ -z "$CORES_PER_NODE" ]; then
    CORES_PER_NODE=4
fi

LCORES_PER_NODE=$(echo "$CORES_PER_NODE * 1" | awk -F* '{print $1*$2}')

# Check if the graph exists
if [ ! -f $GRAPH_PATH ]; then
    echo "Graph file not found: $GRAPH_PATH"
    exit 1
fi

# Check if the symmetric graph exists
if [ ! -f $SYM_GRAPH_PATH ]; then
    echo "Symmetric graph file not found: $SYM_GRAPH_PATH"
    exit 1
fi

# Function to Reset HugePage Allocation 
function reset_hugepage_allocation {
    pkill membw

    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 

    sudo sysctl -w vm.nr_hugepages=0
    sudo sysctl -w vm.nr_overcommit_hugepages=0

    sleep 5

    sudo sysctl -w vm.nr_hugepages=102400

    $EMULATION_SCRIPTS/NUMA_cores.sh

    sleep 5
}

cd $EMULATION_SCRIPTS
sudo ./distNDP_model.sh

cd $RUN_SCRIPTS_DIR
for i in {1..5}; 
do 
    LOG_SUFFIX=GraphQOpt_"$GRAPH_NAME"_3N_run$i.log
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/pagerank/pagerank-push-dist $GRAPH_PATH -exec=Sync  -runs=1 --maxIterations=20 -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/connected-components/connected-components-push-dist $SYM_GRAPH_PATH -symmetricGraph -exec=Sync  -runs=1 -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/sssp/sssp-push-dist $GRAPH_PATH -exec=Sync  -runs=1 -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
done

cd $EMULATION_SCRIPTS
sudo ./dist_model.sh

cd $RUN_SCRIPTS_DIR
for i in {1..5}; 
do
    LOG_SUFFIX=Gluon_"$GRAPH_NAME"_3N_run$i.log
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$CORES_PER_NODE --report-bindings $GALOIS_HOME/pagerank/pagerank-push-dist $GRAPH_PATH -exec=Sync  -runs=1 --maxIterations=20 -t=$CORES_PER_NODE 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$CORES_PER_NODE --report-bindings $GALOIS_HOME/connected-components/connected-components-push-dist $SYM_GRAPH_PATH -symmetricGraph -exec=Sync  -runs=1 -t=$CORES_PER_NODE 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    reset_hugepage_allocation
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by socket:PE=$CORES_PER_NODE --report-bindings $GALOIS_HOME/sssp/sssp-push-dist $GRAPH_PATH -exec=Sync  -runs=1 -t=$CORES_PER_NODE 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;  
done

cd $EMULATION_SCRIPTS
sudo ./disagg_NDP.sh

cd $RUN_SCRIPTS_DIR
# Check if the partition file exists
if [ ! -f $PARTITION_PATH ]; then
    for i in {1..5}; 
    do
        LOG_SUFFIX=Groudon_"$GRAPH_NAME"_3N_run$i.log 
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a pr 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a cc  2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a sssp 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
    done
else
    for i in {1..5}; 
    do
        LOG_SUFFIX=Groudon_"$GRAPH_NAME"_3N_run$i.log 
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a pr -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $SYM_GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a cc -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
        reset_hugepage_allocation
        GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --rankfile $RUN_SCRIPTS_DIR/groudon_proc_map --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m 3 -t $LCORES_PER_NODE -a sssp -p $PARTITION_PATH 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
    done
fi

# for i in {1..3}; 
# do 
#     LOG_SUFFIX=GraphQ_"$GRAPH_NAME"_3N_run$i.log
#     sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/pagerank/pagerank_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
#     sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/cc/cc_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
#     sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
#     GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=$LCORES_PER_NODE --use-hwthread-cpus --report-bindings $GALOIS_HOME/sssp/sssp_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=$LCORES_PER_NODE 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
# done


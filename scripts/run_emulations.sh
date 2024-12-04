#!/bin/bash

set -e # Exit on error
set -x # Print commands

EMULATION_SCRIPTS=/nethome/vrao79/repositories/slow-memory-emulation
GRAPH_PATH=/netscratch/vrao79/galois-graphs/soc-LiveJournal1.gr

GALOIS_HOME="/nethome/vrao79/Galois/build/lonestardist"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/emulation_runs"
PARTITIONS_DIR="/netscratch/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
ALGORITHM=$2
GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)
GRAPH_DIR=$(dirname $GRAPH_PATH)
SYM_GRAPH_PATH=$GRAPH_DIR/../sgr/$GRAPH_NAME.sgr

mkdir -p $LOG_DIR
PWD=$(pwd)

cd $EMULATION_SCRIPTS
sudo ./dist_model.sh

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

cd $PWD
for i in {1..3}; 
do
    LOG_SUFFIX=Gluon_"$GRAPH_NAME"_3N_run$i.log
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=10 --use-hwthread-cpus --report-bindings $GALOIS_HOME/pagerank/pagerank_push $GRAPH_PATH -exec=Sync -runs=1 -t=10 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=10 --use-hwthread-cpus --report-bindings $GALOIS_HOME/cc/cc_push $SYM_GRAPH_PATH -symmetricGraph -exec=Sync -runs=1 -t=10 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=10 --use-hwthread-cpus --report-bindings $GALOIS_HOME/sssp/sssp_push $GRAPH_PATH -exec=Sync -runs=1 -t=10 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX; 
done

cd $EMULATION_SCRIPTS
sudo ./distNDP_model.sh

cd $PWD
for i in {1..3}; 
do 
    LOG_SUFFIX=GraphQOpt_"$GRAPH_NAME"_3N_run$i.log
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/pagerank/pagerank_push $GRAPH_PATH -exec=Sync -runs=1 -t=20 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/cc/cc_push $SYM_GRAPH_PATH -symmetricGraph -exec=Sync -runs=1 -t=20 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/sssp/sssp_push $GRAPH_PATH -exec=Sync -runs=1 -t=20 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
done

for i in {1..3}; 
do 
    LOG_SUFFIX=GraphQ_"$GRAPH_NAME"_3N_run$i.log
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/pagerank/pagerank_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=20 2>&1 | tee $LOG_DIR/pr_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/cc/cc_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=20 2>&1 | tee $LOG_DIR/cc_$LOG_SUFFIX;
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches 
    GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 3 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings $GALOIS_HOME/sssp/sssp_push $GRAPH_PATH --symmetricGraph -exec=Sync -runs=1 -partitionAgnostic -metadata=none -t=20 2>&1 | tee $LOG_DIR/sssp_$LOG_SUFFIX;
done

# cd $EMULATION_SCRIPTS
# sudo ./disagg_NDP.sh

# cd $PWD
# for i in {1..3}; do sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n 4 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings ./bin/Debug/Groudon -g $GRAPH_PATH -c 1 -m 3 -t 20  2>&1 | tee logs/pr_disaggNDP_socLJ_3N_run$i.log; done

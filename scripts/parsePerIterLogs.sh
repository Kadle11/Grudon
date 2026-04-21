#!/bin/bash

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/ablation_dm_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"


GRAPH_PATH=$1
ALGORITHM=$2
NUM_PARTITIONS=$3

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

DINDP_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Groudon_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
NO_OFFLOAD_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Disaggregated_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
NAIVE_OFFLOAD_LOG_FILE=$LOG_DIR/"$ALGORITHM"_GroudonNaiveNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log
NDP_ENGINE_LOG_FILE=$LOG_DIR/"$ALGORITHM"_GroudonNDP_"$GRAPH_NAME"_"$NUM_PARTITIONS"N.log

# Check Algorithm
if [ "$ALGORITHM" == "pr" ]; then
    # Parse Galois PR log
    LOG_DIR="$GROUDON_HOME/logs/perIterLogs/PR_$GRAPH_NAME"_1C"$NUM_PARTITIONS"N
    mkdir -p $LOG_DIR

    cat $NAIVE_OFFLOAD_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/NaiveNDPOffload.log
    cat $NDP_ENGINE_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/GroudonNDPOnly.log
    cat $DINDP_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/Groudon.log
fi

if [ "$ALGORITHM" == "sssp" ]; then
    # Parse Galois PR log
    LOG_DIR="$GROUDON_HOME/logs/perIterLogs/SSSP_$GRAPH_NAME"_1C"$NUM_PARTITIONS"N
    mkdir -p $LOG_DIR

    cat $NAIVE_OFFLOAD_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/NaiveNDPOffload.log
    cat $NDP_ENGINE_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/GroudonNDPOnly.log
    cat $DINDP_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/Groudon.log
fi

if [ "$ALGORITHM" == "cc" ]; then
    # Parse Galois PR log
    LOG_DIR="$GROUDON_HOME/logs/perIterLogs/CC_$GRAPH_NAME"_1C"$NUM_PARTITIONS"N
    mkdir -p $LOG_DIR

    cat $NAIVE_OFFLOAD_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/NaiveNDPOffload.log
    cat $NDP_ENGINE_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/GroudonNDPOnly.log
    cat $DINDP_LOG_FILE | grep -F "[Proc 0] Iteration" > $LOG_DIR/Groudon.log
fi

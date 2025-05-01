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
    cat $NO_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, NoOffload," $0}'
    cat $NAIVE_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, NaiveNDPOffload," $0}'
    cat $NDP_ENGINE_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, GroudonNDPOnly," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, Groudon," $0}'
fi

if [ "$ALGORITHM" == "sssp" ]; then
    # Parse Galois PR log
    cat $NO_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, NoOffload," $0}'
    cat $NAIVE_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, NaiveNDPOffload," $0}'
    cat $NDP_ENGINE_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, GroudonNDPOnly," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, Groudon," $0}'
fi

if [ "$ALGORITHM" == "cc" ]; then
    # Parse Galois PR log
    cat $NO_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, NoOffload," $0}'
    cat $NAIVE_OFFLOAD_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, NaiveNDPOffload," $0}'
    cat $NDP_ENGINE_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, GroudonNDPOnly," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, Groudon," $0}'
fi

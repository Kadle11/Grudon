#!/bin/bash

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/deployment_dm_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"


GRAPH_PATH=$1
ALGORITHM=$2

GALOIS_PR_PARSER=$RUN_SCRIPTS_DIR/parseGaloisPRLog.sh
GROUDON_PARSER=$RUN_SCRIPTS_DIR/parseGroudonLog.sh
GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

GLUON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Gluon_"$GRAPH_NAME"_3N_run1.log
DINDP_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Groudon_"$GRAPH_NAME"_3N.log
DISAGGREGATED_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Disaggregated_"$GRAPH_NAME"_3N.log

# Check Algorithm
if [ "$ALGORITHM" == "pr" ]; then
    # Parse Galois PR log
    cat $GLUON_LOG_FILE | grep -i sendbytes | awk -F, '{print $6*2}'| awk -v graph="$GRAPH_NAME" '{print graph ", PR, Distributed, " $0}'
    cat $DISAGGREGATED_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, Disaggregated," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", PR, DiNDP," $0}'
fi

if [ "$ALGORITHM" == "sssp" ]; then
    # Parse Galois PR log
    cat $GLUON_LOG_FILE | grep -i sendbytes | awk -F, '{print $6*2}'| awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, Distributed, " $0}'
    cat $DISAGGREGATED_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, Disaggregated," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", SSSP, DiNDP," $0}'
fi

if [ "$ALGORITHM" == "cc" ]; then
    # Parse Galois PR log
    cat $GLUON_LOG_FILE | grep -i sendbytes | awk -F, '{print $6*2}'| awk -v graph="$GRAPH_NAME" '{print graph ", CC, Distributed, " $0}'
    cat $DISAGGREGATED_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, Disaggregated," $0}'
    cat $DINDP_LOG_FILE | grep -i totalbytes | awk -F, '{print $5}' | awk -v graph="$GRAPH_NAME" '{print graph ", CC, DiNDP," $0}'
fi

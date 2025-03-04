#!/bin/bash

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/runtime_zoom_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"


GRAPH_PATH=$1
ALGORITHM=$2
POWERLOGS=$3
CONFIG=$4

# Runtime Parsers
GALOIS_PR_PARSER=$RUN_SCRIPTS_DIR/parseGaloisPRLog.sh
GROUDON_PARSER=$RUN_SCRIPTS_DIR/parseGroudonZoomLog.sh

# Power Parsers
GALOIS_POWER_PARSER=$RUN_SCRIPTS_DIR/parseGaloisPowerLog.sh
GROUDON_POWER_PARSER=$RUN_SCRIPTS_DIR/parseGroudonPowerLog.sh

GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

if [ -z "$POWERLOGS" ]; then
    POWERLOGS="false"
fi

echo "Graph, Deployment, Run, Runtime, HostSyncTime, NDPSyncTime, HostIdleTime, NDPIdleTime, MaxUpdate, MaxTraversal, BytesMoved"

if [ $POWERLOGS == "false" ]; then

    for i in {1..3}
    do
        GROUDON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Config"$CONFIG"_Groudon_"$GRAPH_NAME"_3N_run$i.log


        # Check Algorithm
        if [ "$ALGORITHM" == "pr" ]; then
            # Parse Galois PR log
            $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
        fi

        if [ "$ALGORITHM" == "sssp" ]; then
            # Parse Galois PR log
            $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
        fi

        if [ "$ALGORITHM" == "cc" ]; then
            # Parse Galois PR log
            $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
        fi
    done
else
    for i in {4..5}
    do
        GRAPHQ_LOG_FILE=$LOG_DIR/"$ALGORITHM"_GraphQOpt_"$GRAPH_NAME"_3N_run$i.log
        GLUON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Gluon_"$GRAPH_NAME"_3N_run$i.log
        GROUDON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Groudon_"$GRAPH_NAME"_3N_run$i.log
        FAM_LOG_FILE=$LOG_DIR/"$ALGORITHM"_FAMGraph_"$GRAPH_NAME"_3N_run$i.log

        # Parse Galois PR log
        $GALOIS_POWER_PARSER $GLUON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Gluon, " var ", " $0}'
        $GALOIS_POWER_PARSER $GRAPHQ_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", GraphQOpt, " var ", " $0}'
        $GROUDON_POWER_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
        $GROUDON_POWER_PARSER $FAM_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", FAM-Graph, " var ", " $0}'

    done
fi
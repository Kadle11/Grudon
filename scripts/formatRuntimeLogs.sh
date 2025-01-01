#!/bin/bash

GALOIS_HOME="/nethome/vrao79/Groudon/extern/Galois/build/lonestar/analytics/distributed"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs/emulation_runs"
PARTITIONS_DIR="/nethome/vrao79/galois-graphs/partitions"
RUN_SCRIPTS_DIR="$GROUDON_HOME/scripts"


GRAPH_PATH=$1
ALGORITHM=$2

GALOIS_PR_PARSER=$RUN_SCRIPTS_DIR/parseGaloisPRLog.sh
GROUDON_PARSER=$RUN_SCRIPTS_DIR/parseGroudonLog.sh
GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

for i in {1..3}
do
    GRAPHQ_LOG_FILE=$LOG_DIR/"$ALGORITHM"_GraphQOpt_"$GRAPH_NAME"_3N_run$i.log
    GLUON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Gluon_"$GRAPH_NAME"_3N_run$i.log
    GROUDON_LOG_FILE=$LOG_DIR/"$ALGORITHM"_Groudon_"$GRAPH_NAME"_3N_run$i.log

    # Check Algorithm
    if [ "$ALGORITHM" == "pr" ]; then
        # Parse Galois PR log
        $GALOIS_PR_PARSER $GLUON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Gluon, " var ", " $0}'
        $GALOIS_PR_PARSER $GRAPHQ_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", GraphQOpt, " var ", " $0}'
        $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
    fi

    if [ "$ALGORITHM" == "sssp" ]; then
        # Parse Galois PR log
        cat $GLUON_LOG_FILE | grep -E '(Timer_0|, SSSP_0|Sync_SSSP_0)' | awk -F, '{print $6}' | tr "\n" "," | sed 's/,$/\n/g' | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Gluon, " var ", " $0}'
        cat $GRAPHQ_LOG_FILE | grep -E '(Timer_0|, SSSP_0|Sync_SSSP_0)' | awk -F, '{print $6}' | tr "\n" "," | sed 's/,$/\n/g' | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", GraphQOpt, " var ", " $0}'
        $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
    fi

    if [ "$ALGORITHM" == "cc" ]; then
        # Parse Galois PR log
        cat $GLUON_LOG_FILE | grep -E '(Timer_0|, ConnectedComp_0|Sync_ConnectedComp_0)' | awk -F, '{print $6}' | tr "\n" "," | sed 's/,$/\n/g' | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Gluon, " var ", " $0}'
        cat $GRAPHQ_LOG_FILE | grep -E '(Timer_0|, ConnectedComp_0|Sync_ConnectedComp_0)' | awk -F, '{print $6}' | tr "\n" "," | sed 's/,$/\n/g' | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", GraphQOpt, " var ", " $0}'
        $GROUDON_PARSER $GROUDON_LOG_FILE | awk -v var="$i" -v graph="$GRAPH_NAME" '{print graph ", Groudon, " var ", " $0}'
    fi
done

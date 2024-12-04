#!/bin/bash

# This script is used to sweep over different partitioning sizes and run the program

set -e # Exit on error
set -x # Print commands

GALOIS_HOME="/nethome/vrao79/Galois/build/lonestardist"
GROUDON_HOME="/nethome/vrao79/Groudon"
GROUDON_BIN="$GROUDON_HOME/build/bin/Debug/Groudon"
LOG_DIR="$GROUDON_HOME/logs"
PARTITIONS_DIR="/netscratch/vrao79/galois-graphs/partitions"

GRAPH_PATH=$1
ALGORITHM=$2
GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

# Check Args
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <graph_path> <algorithm>"
    exit 1
fi

# Check if the graph exists
if [ ! -f $GRAPH_PATH ]; then
    echo "Graph file not found: $GRAPH_PATH"
    exit 1
fi

echo "Running $ALGORITHM on graph: $GRAPH_NAME"

mkdir -p $LOG_DIR

# Run the Baselines (Galois)
GALOIS_BIN=""

if [ "$ALGORITHM" == "bfs" ]
then

GALOIS_BIN="$GALOIS_HOME/bfs/bfs_push"

elif [ "$ALGORITHM" == "sssp" ]
then

GALOIS_BIN="$GALOIS_HOME/sssp/sssp_push"

elif [ "$ALGORITHM" == "pr" ]
then

GALOIS_BIN="$GALOIS_HOME/pagerank/pagerank_push"

elif [ "$ALGORITHM" == "cc" ]
then

GALOIS_BIN="$GALOIS_HOME/cc/cc_push"

fi

# Check if Executable exists
if [ ! -f "$GALOIS_BIN" ]; then
    echo "Galois Executable not found: $GALOIS_BIN"
    exit 1
fi

if [ $ALGORITHM == "cc" ]; then
    GALOIS_BIN="$GALOIS_BIN -symmetricGraph"
fi

echo "Galois Executable: $GALOIS_BIN"

# Run Galois
for i in {2..8}
do
    echo "Running Galois with $i partitions"
    LOG_FILE=$LOG_DIR/Gluon-$GRAPH_NAME-$ALGORITHM-1C"$i"M5T.log
    OUTPUT=$(GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $i --npersocket 1 --map-by NUMA:PE=10 --use-hwthread-cpus --report-bindings $GALOIS_BIN -exec=Sync -runs=1 $GRAPH_PATH -t=20 2>&1 | tee $LOG_FILE)
done

# Run Groudon
for i in {1..8}
do
    echo "Running with $i partitions"
    LOG_FILE=$LOG_DIR/Groudon-$GRAPH_NAME-$ALGORITHM-1C"$i"M5T.log
    PARTITIONS_FILE=$PARTITIONS_DIR/$GRAPH_NAME."$i"parts
    TOTAL_PARTS=$((i + 1))
    OUTPUT=$(GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $TOTAL_PARTS --npersocket 1 --map-by NUMA:PE=7 --use-hwthread-cpus --report-bindings $GROUDON_BIN -g $GRAPH_PATH -c 1 -m $i -t 20 -p $PARTITIONS_FILE 2>&1 | tee $LOG_FILE)
done

# Run ~GraphQ
for i in {2..8}
do
    echo "Running Galois with $i partitions"
    LOG_FILE=$LOG_DIR/GraphQ-$GRAPH_NAME-$ALGORITHM-1C"$i"M5T.log
    OUTPUT=$(GALOIS_DO_NOT_BIND_THREADS=1 time mpirun -n $i --npersocket 1 --map-by NUMA:PE=10 --use-hwthread-cpus --report-bindings $GALOIS_BIN $GRAPH_PATH -partitionAgnostic -metadata=none -exec=Sync -runs=1 -t=20 2>&1 | tee $LOG_FILE)
done

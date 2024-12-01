#!/bin/bash

set -e
set -x

GROUDON_HOME="/netscratch/vrao79/Groudon"
PARTITIONER="$GROUDON_HOME/build/partitioner"

GRAPH_PATH=$1
GRAPH_NAME=$(basename $GRAPH_PATH | cut -d'.' -f1)

PARTITIONS_DIR=/netscratch/vrao79/galois-graphs/partitions



for i in {2..10}
do
 ./mtmetis $GRAPH_PATH $i $PARTITIONS_DIR/$GRAPH_NAME."$i"parts
done

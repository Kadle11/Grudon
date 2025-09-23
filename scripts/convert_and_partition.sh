#!/bin/bash

set -x

MTX_PATH=$1
NUM_PARTITIONS=$2
PARTITIONS_DIR=$3

GALOIS_GRAPH_PATH=./graphs/galois
GRAPH_CONVERTER_PATH=./extern/Galois/build/tools/graph-convert/graph-convert
PARTITIONER_PATH=./extern/Galois/build/lonestar/analytics/distributed/partition/partition-dist

mkdir -p $GALOIS_GRAPH_PATH/gr
mkdir -p $GALOIS_GRAPH_PATH/sgr

# Extract Graph Name

GRAPH_NAME=$(basename $1 .sgr)
GR_PATH=$GALOIS_GRAPH_PATH/gr/$GRAPH_NAME.gr
SGR_PATH=$GALOIS_GRAPH_PATH/sgr/$GRAPH_NAME.sgr

# MTX2GR
# $GRAPH_CONVERTER_PATH --mtx2gr --edgeType=uint32 $MTX_PATH $GR_PATH

# GR2SGR
# $GRAPH_CONVERTER_PATH --gr2sgr --edgeType=uint32 $GR_PATH $SGR_PATH

# Partitioning
mpirun -n $NUM_PARTITIONS $PARTITIONER_PATH $SGR_PATH --symmetricGraph --output_path=$PARTITIONS_DIR/$GRAPH_NAME."$NUM_PARTITIONS"parts
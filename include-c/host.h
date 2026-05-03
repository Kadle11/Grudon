#ifndef HOST_H
#define HOST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "device.h"
#include "types.h"

#define MAX_ITERATIONS 1000

// Read graph from file and construct CXL_Graph structure
CXL_Graph* read_graph(const char* filename);

// Initialize PageRank vertex properties and frontier
void init_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier);

// Quicksort function to sort vertices by PageRank score for final output
void quicksort(RankPair* arr, size_t count);

#endif  // HOST_H

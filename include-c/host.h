#ifndef HOST_H
#define HOST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <limits.h>

#include "device.h"
#include "types.h"

#define MAX_ITERATIONS 1000

// Read graph from file and construct CXL_Graph structure.
// When build_symmetric is non-zero, also materialize reverse edges.
CXL_Graph* read_graph(const char* filename, int build_symmetric);

// Initialize PageRank vertex properties and frontier
void init_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier);

// Initialize Connected Components vertex properties and frontier
void init_connected_components(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier);

// Initialize Single-Source Shortest Paths (SSSP) vertex properties and frontier
void init_sssp(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier);

// Apply updates to SSSP vertex properties and determine next frontier
int apply_updates_sssp(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host);

// Apply updates to vertex properties and determine next frontier
int apply_updates_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host);

// Apply updates to Connected Components vertex properties and determine next frontier
int apply_updates_connected_components(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host);

// Quicksort function to sort vertices by PageRank score for final output
void quicksort(RankPair* arr, size_t count);

#endif  // HOST_H

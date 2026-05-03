#ifndef HOST_H
#define HOST_H

#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "device.h"
#include "types.h"

#define MAX_ITERATIONS 1000

CXL_Graph* read_graph(const char* filename);
void init_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier);
void quicksort(RankPair* arr, size_t count)

#endif  // HOST_H

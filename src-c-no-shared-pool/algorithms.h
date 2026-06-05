#ifndef ALGORITHMS_H
#define ALGORITHMS_H

#include "../include-c/types.h"
#include "graph_io.h"

typedef enum
{
	ALGO_PAGE_RANK,
	ALGO_CONNECTED_COMPONENTS,
	ALGO_SSSP
} AlgorithmKind;

size_t run_pagerank(const CXL_Graph* graph, VProp* props);
size_t run_connected_components(const CXL_Graph* graph, VProp* props);
size_t run_sssp(const CXL_Graph* graph, VProp* props);

#endif // ALGORITHMS_H

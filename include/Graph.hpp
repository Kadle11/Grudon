#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <galois/Galois.h>
#include <galois/graphs/LCGraph.h>
#include <galois/graphs/TypeTraits.h>
#include <galois/DynamicBitset.h>

using Graph = galois::graphs::LC_CSR_Graph<uint64_t, uint32_t>::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

enum NODE_TYPE
{
    COMPUTE_NODE,
    MEMORY_NODE,
    SWITCH_NODE
};

#endif  // GRAPH_HPP
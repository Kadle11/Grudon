#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <galois/DynamicBitset.h>
#include <galois/Galois.h>
#include <galois/graphs/LCGraph.h>
#include <galois/graphs/TypeTraits.h>

using Graph = galois::graphs::LC_CSR_Graph<uint64_t, void>::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

enum NODE_TYPE
{
  COMPUTE_NODE,
  MEMORY_NODE,
  SWITCH_NODE
};

enum OFFLOAD_DECISION
{
  NDP_OFFLOAD,
  NO_OFFLOAD,
  INC_OFFLOAD,
};

#endif  // GRAPH_HPP
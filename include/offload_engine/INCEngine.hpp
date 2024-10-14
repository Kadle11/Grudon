#ifndef INC_ENGINE_HPP
#define INC_ENGINE_HPP

#include "DistributedGraph.hpp"
#include "GraphAlgorithm.hpp"
#include "Logger.hpp"

// TODO: Are Galois Primitives faster than Std Primitives?

OFFLOAD_DECISION INCEngine(
    std::vector<GNode>& frontier,
    galois::LargeArray<uint64_t>& out_degrees,
    DistributedGraph& distributed_graph,
    uint64_t& offload_threshold
  )
{

  size_t offload_factor = frontier.size();
  for (GNode& lid : frontier)
  {
    offload_factor += out_degrees[distributed_graph.getGlobalNode(lid)];
  }

  if (offload_factor > offload_threshold)
  {
    return INC_OFFLOAD;
  }

  return NO_OFFLOAD;
}

#endif  // INC_ENGINE_HPP

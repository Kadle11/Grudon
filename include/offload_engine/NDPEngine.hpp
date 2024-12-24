#ifndef NDP_ENGINE_HPP
#define NDP_ENGINE_HPP

#include <execution>

#include "DistributedGraph.hpp"
#include "GraphAlgorithm.hpp"
#include "Logger.hpp"

// TODO: Are Galois Primitives faster than Std Primitives?

OFFLOAD_DECISION NDPEngine(
    std::vector<GNode>& frontier,
    galois::LargeArray<bool>& coverage_vector,
    size_t& offload_threshold,
    DistributedGraph& distributed_graph,
    uint32_t& num_memory)
{
  if (frontier.size() == 0 | frontier.size() > offload_threshold)
  {
    return NDP_OFFLOAD;
  }

  if (!std::all_of(
          std::execution::par,
          frontier.begin(),
          frontier.end(),
          [&](const GNode& lid) { return coverage_vector[lid]; }))
  {
    return NDP_OFFLOAD;
  }

  double skewness = calculateSkew(frontier, num_memory, distributed_graph);
  if (skewness > 0.5 || skewness < -0.5)
  {
    return NDP_OFFLOAD;
  }

  spdlog::info("No Offload, Frontier Size: {}", frontier.size());

  return NO_OFFLOAD;
}

#endif  // NDP_ENGINE_HPP

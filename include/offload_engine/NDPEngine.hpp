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
    galois::LargeArray<uint64_t>& out_degrees,
    size_t& offload_threshold,
    uint32_t& num_memory,
    uint32_t& num_compute,
    double& offload_coeff,
    double& fetch_coeff,
    double& decision_coeff,
    uint64_t& neighbor_count,
    uint64_t& frontier_size)
{
  frontier_size = frontier.size();
  if (frontier_size == 0 | frontier_size > offload_threshold)
  {
    return NDP_OFFLOAD;
  }

  if (num_compute > 1 &&
      !std::all_of(
          std::execution::par, frontier.begin(), frontier.end(), [&](const GNode& lid) { return coverage_vector[lid]; }))
  {
    return NDP_OFFLOAD;
  }

  galois::GAccumulator<uint64_t> neighbor_accum;
  neighbor_accum.reset();
  galois::do_all(
      galois::iterate(frontier.begin(), frontier.end()),
      [&](const GNode& lid) { neighbor_accum += out_degrees[lid]; },
      galois::loopname("Calculate Neighbor Accumulator"),
      galois::no_stats(),
      galois::steal());

  neighbor_count = neighbor_accum.reduce();

  if ((frontier_size + neighbor_count) > offload_threshold)
  {
    return NDP_OFFLOAD;
  }

  decision_coeff += 0.7 * offload_coeff - 0.5 * fetch_coeff;
  
  spdlog::info(
      "[Proc 0] Frontier Size: {}, Decision Coefficient: {}/{}/{}",
      frontier_size,
      decision_coeff,
      offload_coeff,
      fetch_coeff);

  if (decision_coeff >= -1)
  {
    return NDP_OFFLOAD;
  }

  return NO_OFFLOAD;
}

#endif  // NDP_ENGINE_HPP

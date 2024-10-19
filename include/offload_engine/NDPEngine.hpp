#ifndef NDP_ENGINE_HPP
#define NDP_ENGINE_HPP

#include "DistributedGraph.hpp"
#include "GraphAlgorithm.hpp"
#include "Logger.hpp"

// TODO: Are Galois Primitives faster than Std Primitives?

OFFLOAD_DECISION NDPEngine(
    galois::ThreadSafeOrderedSet<GNode>& frontier,
    galois::LargeArray<bool>& coverage_vector,
    size_t& offload_threshold,
    DistributedGraph& distributed_graph,
    uint32_t& num_memory)
{
  if (frontier.size() == 0 | frontier.size() > offload_threshold)
  {
    return NDP_OFFLOAD;
  }

  if (!std::all_of(frontier.begin(), frontier.end(), [&](GNode v) { return coverage_vector[v]; }))
  {
    return NDP_OFFLOAD;
  }

  std::vector<uint64_t> mirrorCoverage(num_memory, 0);

  for (const GNode& lid : frontier)
  {
    GNode gid = distributed_graph.getGlobalNode(lid);
    uint32_t worker_id = distributed_graph.getMirrorPartition(gid);

    mirrorCoverage[worker_id]++;
  }

  uint64_t maxMirrorCoverage = *std::max_element(mirrorCoverage.begin(), mirrorCoverage.end());
  uint64_t totalMirrorCoverage = std::accumulate(mirrorCoverage.begin(), mirrorCoverage.end(), 0);
  uint64_t memory_nodes_touched =
      std::count_if(mirrorCoverage.begin(), mirrorCoverage.end(), [](uint64_t v) { return v > 0; });

  if (memory_nodes_touched == 0 || maxMirrorCoverage < (totalMirrorCoverage / memory_nodes_touched))
  {
    return NDP_OFFLOAD;
  }

  // spdlog::info("No Offload, Frontier Size: {}", frontier.size());

  return NO_OFFLOAD;
}

#endif  // NDP_ENGINE_HPP

#ifndef INC_ENGINE_HPP
#define INC_ENGINE_HPP

#include <math.h>

#include "DistributedGraph.hpp"
#include "GraphAlgorithm.hpp"
#include "Logger.hpp"

#define COMPUTE_TO_MEM_BW    64424509440.0f  // Bytes/Sec
#define HOST_COMPUTE_POWER   210e9           // 210 GOps
#define SWITCH_COMPUTE_RATIO 1 / 25
#define VPROP_SIZE           4  // UINT32, Float

// TODO: Are Galois Primitives faster than Std Primitives?

// Return NO_OFFLOAD or INC_OFFLOAD

OFFLOAD_DECISION INCEngine(
    std::vector<GNode>& frontier,
    galois::LargeArray<uint64_t>& out_degrees,
    DistributedGraph& distributed_graph,
    double& replication_factor,
    uint32_t& num_memory,
    uint64_t& bv_size)
{
  galois::GAccumulator<float> umirrors;
  galois::do_all(
      galois::iterate(frontier),
      [&](const GNode& lid) { umirrors += distributed_graph.unique_mirrors_traversed[lid]; },
      galois::loopname("Count mirrors"),
      galois::no_stats(),
      galois::steal());

  // Reduce and Round Up
  uint64_t unique_mirrors = static_cast<uint64_t>(std::ceil(umirrors.reduce()));

  float approx_tx_time = (unique_mirrors * replication_factor * VPROP_SIZE + bv_size) / COMPUTE_TO_MEM_BW;
  float approx_computation_time = (unique_mirrors * (replication_factor - 1)) / (HOST_COMPUTE_POWER * SWITCH_COMPUTE_RATIO);

  spdlog::info("Tx Time: {}, Compute Time: {}, Unique mirrors: {}", approx_tx_time, approx_computation_time, unique_mirrors);

  if (approx_tx_time > approx_computation_time)
  {
    return INC_OFFLOAD;
  }

  return NO_OFFLOAD;
}
#endif  // INC_ENGINE_HPP

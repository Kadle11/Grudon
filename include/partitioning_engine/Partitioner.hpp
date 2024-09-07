#ifndef PARTITIONER_HPP
#define PARTITIONER_HPP

#include "Graph.hpp"
#include "Logger.hpp"

void NaivePartitioner(
    Graph& graph,
    uint32_t num_masters,
    uint32_t num_mirrors,
    galois::LargeArray<GNode>& mirror_partition,
    galois::LargeArray<GNode>& master_partition,
    std::vector<uint64_t>& master_partition_sizes,
    std::vector<uint64_t>& mirror_partition_sizes,
    std::vector<uint64_t>& mirror_edge_counts)
{
  master_partition.allocateInterleaved(graph.size());
  mirror_partition.allocateInterleaved(graph.size());

  std::vector<galois::GAccumulator<uint64_t>> master_partition_sizes_accum(num_masters);
  std::vector<galois::GAccumulator<uint64_t>> mirror_partition_sizes_accum(num_mirrors);
  std::vector<galois::GAccumulator<uint64_t>> mirror_edge_counts_accum(num_mirrors);

  galois::do_all(
      galois::iterate(graph),
      [&](GNode n)
      {
        master_partition[n] = n % num_masters;
        mirror_partition[n] = n % num_mirrors;

        master_partition_sizes_accum[master_partition[n]] += 1;
        mirror_partition_sizes_accum[mirror_partition[n]] += 1;
        mirror_edge_counts_accum[mirror_partition[n]] += std::distance(graph.edge_begin(n), graph.edge_end(n));
      },
      galois::loopname("Naive Partitioner"));

  for (uint32_t i = 0; i < num_masters; ++i)
  {
    master_partition_sizes[i] = master_partition_sizes_accum[i].reduce();
  }

  for (uint32_t i = 0; i < num_mirrors; ++i)
  {
    mirror_partition_sizes[i] = mirror_partition_sizes_accum[i].reduce();
    mirror_edge_counts[i] = mirror_edge_counts_accum[i].reduce();
  }

  // Log Partition Sizes
  spdlog::info("Master Partition Sizes: {}", fmt_array(master_partition_sizes));
  spdlog::info("Mirror Partition Sizes: {}", fmt_array(mirror_partition_sizes));

  // Clear Accumulators
  master_partition_sizes_accum.clear();
  mirror_partition_sizes_accum.clear();
  mirror_edge_counts_accum.clear();
}

#endif  // PARTITIONER_HPP
#ifndef PARTITIONER_HPP
#define PARTITIONER_HPP

#include <fstream>

#include "Graph.hpp"
#include "Logger.hpp"
#include "metis.h"

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
  master_partition.allocateLocal(graph.size());
  mirror_partition.allocateLocal(graph.size());

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

void METISPartitioner(
    Graph& graph,
    uint32_t num_masters,
    uint32_t num_mirrors,
    galois::LargeArray<GNode>& mirror_partition,
    galois::LargeArray<GNode>& master_partition,
    std::vector<uint64_t>& master_partition_sizes,
    std::vector<uint64_t>& mirror_partition_sizes,
    std::vector<uint64_t>& mirror_edge_counts,
    std::string partitioning_scheme_file = "")
{
  if (num_mirrors == 1)
  {
    NaivePartitioner(
        graph,
        num_masters,
        num_mirrors,
        mirror_partition,
        master_partition,
        master_partition_sizes,
        mirror_partition_sizes,
        mirror_edge_counts);
    return;
  }

  std::vector<idx_t> partitions(graph.size());

  if (!partitioning_scheme_file.empty())
  {
    std::ifstream file(partitioning_scheme_file);
    if (!file.is_open())
    {
      spdlog::error("Failed to open partitioning scheme file: {}", partitioning_scheme_file);
      return;
    }

    // Each line in the file is a partition assignment for a node
    for (idx_t i = 0; i < graph.size(); ++i)
    {
      file >> partitions[i];
    }

    spdlog::info("Partitioning Scheme Loaded from File: {}", partitioning_scheme_file);
  }
  else
  {
    std::vector<idx_t> xadj;
    std::vector<idx_t> adjncy;

    idx_t num_nodes = graph.size();

    xadj.reserve(num_nodes + 1);

    idx_t curr_offset = 0;
    xadj.push_back(curr_offset);

    for (auto n : graph)
    {
      curr_offset += std::distance(graph.edges(n).begin(), graph.edges(n).end());
      xadj.push_back(curr_offset);

      for (auto e : graph.edges(n))
      {
        adjncy.push_back(graph.getEdgeDst(e));
      }
    }

    idx_t options[METIS_NOPTIONS];

    idx_t ncon = 1;
    idx_t nparts = num_mirrors;
    idx_t objval;

    METIS_SetDefaultOptions(options);

    int res = METIS_PartGraphKway(
        &num_nodes,
        &ncon,
        xadj.data(),
        adjncy.data(),
        NULL,
        NULL,
        NULL,
        &nparts,
        NULL,
        NULL,
        options,
        &objval,
        partitions.data());

    if (res == METIS_OK)
    {
      spdlog::info("METIS Partitioning Complete\n");
    }
    else
    {
      spdlog::error("METIS Partitioning Failed\n");
    }
  }

  master_partition.allocateLocal(graph.size());
  mirror_partition.allocateLocal(graph.size());

  std::vector<galois::GAccumulator<uint64_t>> master_partition_sizes_accum(num_masters);
  std::vector<galois::GAccumulator<uint64_t>> mirror_partition_sizes_accum(num_mirrors);
  std::vector<galois::GAccumulator<uint64_t>> mirror_edge_counts_accum(num_mirrors);

  galois::do_all(
      galois::iterate(graph),
      [&](GNode n)
      {
        master_partition[n] = n % num_masters;
        mirror_partition[n] = partitions[n];

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
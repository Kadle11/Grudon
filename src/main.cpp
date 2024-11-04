#include <galois/Galois.h>
#include <numa.h>
#include <spdlog/spdlog.h>

#include <cxxopts.hpp>

#include "DistributedGraph.hpp"
#include "Graph.hpp"
#include "GraphAlgorithm.hpp"
#include "MPI.hpp"
#include "Workers.hpp"
#include "graph_algorithms/pr.hpp"
#include "graph_algorithms/sssp.hpp"
#include "graph_algorithms/cc.hpp"

int main(int argc, char **argv)
{
  MPICore net(argc, argv);
  galois::SharedMemSys G;

  cxxopts::Options options("Skywalker", "A Graph Processing Framework");
  options.add_options()("g, graph-path", "Input graph file", cxxopts::value<std::string>())(
      "c, num-compute", "Number of compute nodes", cxxopts::value<size_t>())(
      "m, num-memory", "Number of memory nodes", cxxopts::value<size_t>())(
      "t, threads", "Number of threads", cxxopts::value<size_t>()->default_value("1"))("h, help", "Print usage");

  std::string graph_path = "";
  size_t num_compute = 0;
  size_t num_memory = 0;
  size_t num_threads = 1;

  try
  {
    auto result = options.parse(argc, argv);

    if (result.count("help"))
    {
      std::cout << options.help() << std::endl;
      return 0;
    }

    if (result.count("graph-path"))
    {
      graph_path = result["graph-path"].as<std::string>();
    }
    else
    {
      std::cerr << "Please provide a graph path" << std::endl;
      return 1;
    }

    if (result.count("num-compute"))
    {
      num_compute = result["num-compute"].as<size_t>();
    }
    else
    {
      std::cerr << "Please provide the number of master nodes" << std::endl;
      return 1;
    }

    if (result.count("num-memory"))
    {
      num_memory = result["num-memory"].as<size_t>();
    }
    else
    {
      std::cerr << "Please provide the number of mirror nodes" << std::endl;
      return 1;
    }

    if (result.count("threads"))
    {
      num_threads = result["threads"].as<size_t>();
    }
  }
  catch (const std::exception &e)
  {
    std::cerr << "Error parsing options: " << e.what() << std::endl;
    return 1;
  }

  num_threads = galois::setActiveThreads(num_threads);

  uint32_t node_id = net.getRank();
  NODE_TYPE node_type = (node_id < num_compute) ? COMPUTE_NODE : MEMORY_NODE;

  std::vector<galois::DynamicBitSet> bitCommVector;
  std::vector<std::unordered_map<GNode, GNode>> rTranslationTable;
  std::vector<std::unordered_map<GNode, GNode>> sTranslationTable;
  galois::LargeArray<uint64_t> out_degrees;
  galois::LargeArray<bool> coverage_vector;
  uint64_t num_vertices = 0;
  uint64_t total_vertices = 0;
  uint64_t num_edges = 0;

  if (node_id == 0)
  {
    // Log Configuration
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v ");
    spdlog::set_level(spdlog::level::info);
    spdlog::info("Graph Path: {}", graph_path);
    spdlog::info("Number of Compute Nodes: {}", num_compute);
    spdlog::info("Number of Memory Nodes: {}", num_memory);
    spdlog::info("Number of Threads: {}", num_threads);
  }

  // DistributedGraph distributed_graph(
  //     graph_path,
  //     num_compute,
  //     num_memory,
  //     num_vertices,
  //     total_vertices,
  //     num_edges,
  //     node_id,
  //     node_type,
  //     bitCommVector,
  //     sTranslationTable,
  //     rTranslationTable,
  //     out_degrees,
  //     coverage_vector,
  //     net);

  // distributed_graph.printState();

  GraphAlgorithm<float_t> *graph_algorithm = new PageRank<float_t>(node_type, "PageRank", graph_path, num_compute, num_memory, node_id,
  net);

  graph_algorithm->init();
  graph_algorithm->run();
  graph_algorithm->printState();

  delete graph_algorithm;

  return 0;
}

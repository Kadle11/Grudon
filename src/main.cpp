#include <galois/Galois.h>
#include <numa.h>
#include <spdlog/spdlog.h>

#include <cxxopts.hpp>

#include "DistributedGraph.hpp"
#include "Graph.hpp"
#include "GraphAlgorithm.hpp"
#include "MPI.hpp"
#include "Workers.hpp"
#include "graph_algorithms/cc.hpp"
#include "graph_algorithms/pr.hpp"
#include "graph_algorithms/sssp.hpp"

int main(int argc, char **argv)
{
  MPICore net(argc, argv);
  galois::SharedMemSys G;

  cxxopts::Options options("Skywalker", "A Graph Processing Framework");
  options.add_options()("g, graph-path", "Input graph file", cxxopts::value<std::string>())(
      "c, num-compute", "Number of compute nodes", cxxopts::value<size_t>())(
      "m, num-memory", "Number of memory nodes", cxxopts::value<size_t>())(
      "t, threads", "Number of threads", cxxopts::value<size_t>()->default_value("1"))(
      "p, partitioning-scheme", "Partitioning scheme file", cxxopts::value<std::string>())(
      "a, algorithm", "Graph Algorithm", cxxopts::value<std::string>()->default_value("pr"))(
      "o, offload-mode", "Offload Mode", cxxopts::value<uint32_t>())(
      "max-iterations", "Maximum number of iterations", cxxopts::value<uint32_t>()->default_value("1000"))(
      "h, help", "Print usage");

  std::string graph_path = "";
  size_t num_compute = 0;
  size_t num_memory = 0;
  size_t num_threads = 1;
  std::string partitioning_scheme_file = "";
  std::string algorithm = "";
  uint32_t offload_mode = 0;
  uint32_t max_iterations = 1000;

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

    if (result.count("partitioning-scheme"))
    {
      partitioning_scheme_file = result["partitioning-scheme"].as<std::string>();
    }

    if (result.count("algorithm"))
    {
      algorithm = result["algorithm"].as<std::string>();
      if (algorithm != "pr" && algorithm != "cc" && algorithm != "sssp")
      {
        std::cerr << "Invalid algorithm. Please choose from pr, cc, sssp" << std::endl;
        return 1;
      }
    }

    if (result.count("offload-mode"))
    {
      offload_mode = result["offload-mode"].as<uint32_t>();
    } 
    else
    {
      offload_mode = 1;
    }

    if (result.count("max-iterations"))
    {
      max_iterations = result["max-iterations"].as<uint32_t>();
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
  std::vector<std::vector<GNode>> rTranslationTable;
  std::vector<std::vector<GNode>> sTranslationTable;
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

  if (algorithm == "pr")
  {
    GraphAlgorithm<float> *graph_algorithm = new PageRank<float>(
        node_type, "PageRank", graph_path, num_compute, num_memory, node_id, net, partitioning_scheme_file);

    graph_algorithm->init();
    graph_algorithm->run(offload_mode, max_iterations);

    graph_algorithm->verify();

    delete graph_algorithm;
  }
  else if (algorithm == "cc")
  {
    GraphAlgorithm<uint32_t> *graph_algorithm = new CC<uint32_t>(
        node_type, "ConnectedComponents", graph_path, num_compute, num_memory, node_id, net, partitioning_scheme_file);

    graph_algorithm->init();
    graph_algorithm->run(offload_mode, max_iterations);

    graph_algorithm->verify();

    delete graph_algorithm;
  }
  else if (algorithm == "sssp")
  {
    GraphAlgorithm<uint32_t> *graph_algorithm = new SSSP<uint32_t>(
        node_type, "SingleSourceShortestPath", graph_path, num_compute, num_memory, node_id, net, partitioning_scheme_file);

    graph_algorithm->init();
    graph_algorithm->run(offload_mode, max_iterations);

    graph_algorithm->verify();

    delete graph_algorithm;
  }

  return 0;
}

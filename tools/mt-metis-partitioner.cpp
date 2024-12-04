
#include <galois/Galois.h>
#include <galois/graphs/LC_CSR_Graph.h>
#include <galois/graphs/ReadGraph.h>
#include <galois/graphs/TypeTraits.h>

#include <cxxopts.hpp>
#include <fstream>
#include <iostream>

#include "mtmetis.h"

/**
 * Defines and types
 */
using Graph = galois::graphs::LC_CSR_Graph<uint64_t, uint32_t>::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

/**
 * Print usage information
 */
void print_usage(const cxxopts::Options& options)
{
  std::cout << options.help() << std::endl;
  std::cout << "Usage: ./program_name [options] <graphfile> <nparts> [<partfile> | -]" << std::endl;
}

/**
 * Main
 */
int main(int argc, char** argv)
{
  galois::SharedMemSys G;

  cxxopts::Options options("MT-Metis", "A parallel graph partitioning tool");

  // Define options
  options.add_options()("h,help", "Print usage information")(
      "c,ctype", "Coarsening type (e.g., shem, rm)", cxxopts::value<std::string>()->default_value("shem"))(
      "r,rtype", "Refinement type (e.g., greedy, fm)", cxxopts::value<std::string>()->default_value("greedy"))(
      "p,ptype", "Partition type (e.g., kway, nd)", cxxopts::value<std::string>()->default_value("kway"))(
      "n,nparts", "Number of parts", cxxopts::value<int>())(
      "t,threads", "Number of threads", cxxopts::value<int>()->default_value("1"))(
      "input", "Input graph file", cxxopts::value<std::string>())(
      "output", "Output partition file", cxxopts::value<std::string>()->default_value("-"));

  // Parse arguments
  auto result = options.parse(argc, argv);

  if (result.count("help"))
  {
    print_usage(options);
    return 0;
  }

  // Required arguments
  if (!result.count("input") || !result.count("nparts"))
  {
    std::cerr << "Error: input graph file and number of parts are required." << std::endl;
    print_usage(options);
    return 1;
  }

  // Get values
  std::string input_file = result["input"].as<std::string>();
  std::string output_file = result["output"].as<std::string>();
  int num_parts = result["nparts"].as<int>();
  int num_threads = result["threads"].as<int>();

  galois::setActiveThreads(num_threads);

  // Read the graph
  Graph graph;
  galois::graphs::readGraph(graph, input_file);

  size_t num_nodes = graph.size();
  size_t num_edges = graph.sizeEdges();
  std::cout << "Graph loaded: " << num_nodes << " nodes." << std::endl;

  mtmetis_vtx_type nvtxs = num_nodes;
  mtmetis_adj_type* xadj = NULL;
  mtmetis_vtx_type* adjncy = NULL;
  mtmetis_pid_type* where = NULL;
  double* opts = NULL;

  // Allocate memory
  xadj = (mtmetis_adj_type*)malloc((nvtxs + 1) * sizeof(mtmetis_adj_type));
  adjncy = (mtmetis_vtx_type*)malloc(2 * num_edges * sizeof(mtmetis_vtx_type));
  where = (mtmetis_pid_type*)malloc(nvtxs * sizeof(mtmetis_pid_type));

  std::vector<mtmetis_pid_type> partition(num_nodes, 0);

  // Fill in xadj and adjncy
  mtmetis_vtx_type edge_count = 0;
  for (GNode src : graph)
  {
    xadj[src] = edge_count;
    for (auto edge : graph.edges(src))
    {
      GNode dst = graph.getEdgeDst(edge);
      adjncy[edge_count++] = dst;
    }
  }

  opts = mtmetis_init_options();
  opts[MTMETIS_OPTION_VERBOSITY] = MTMETIS_VERBOSITY_MAXIMUM;
  opts[MTMETIS_OPTION_NPARTS] = num_parts;
  opts[MTMETIS_OPTION_IGNORE] = MTMETIS_IGNORE_EDGEWEIGHTS | MTMETIS_IGNORE_VERTEXWEIGHTS;
  opts[MTMETIS_OPTION_PTYPE] = MTMETIS_PTYPE_KWAY;
  opts[MTMETIS_OPTION_SEED] = 0;

  mtmetis_partition_explicit(nvtxs, xadj, adjncy, NULL, NULL, opts, where, NULL);

  partition.assign(where, where + nvtxs);

  // Write output
  if (output_file == "-")
  {
    for (const auto& p : partition)
    {
      std::cout << p << "\n";
    }
  }
  else
  {
    std::ofstream fout(output_file);
    if (!fout)
    {
      std::cerr << "Error: could not open output file for writing." << std::endl;
      return 1;
    }
    for (const auto& p : partition)
    {
      fout << p << "\n";
    }
    fout.close();
  }

  // Cleanup
  free(xadj);
  free(adjncy);
  free(opts);
  partition.clear();

  std::cout << "Partitioning completed." << std::endl;
  return 0;
}

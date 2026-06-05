#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include-c/types.h"
#include "graph_io.h"
#include "algorithms.h"

static AlgorithmKind parse_algorithm(const char* name)
{
  if (strcmp(name, "pr") == 0 || strcmp(name, "pagerank") == 0)
  {
    return ALGO_PAGE_RANK;
  }
  if (strcmp(name, "cc") == 0 || strcmp(name, "components") == 0 || strcmp(name, "connected_components") == 0)
  {
    return ALGO_CONNECTED_COMPONENTS;
  }
  if (strcmp(name, "sssp") == 0 || strcmp(name, "shortest_path") == 0)
  {
    return ALGO_SSSP;
  }
  return (AlgorithmKind)-1;
}

int main(int argc, char* argv[])
{
  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <graph_file.mtx> <algorithm> [--symmetric|-s]\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char* graph_file = argv[1];
  const char* algorithm_name = argv[2];
  int build_symmetric = 0;

  const char* graph_base = strrchr(graph_file, '/');
  graph_base = graph_base ? graph_base + 1 : graph_file;

  AlgorithmKind algorithm = parse_algorithm(algorithm_name);
  if (algorithm == ALGO_CONNECTED_COMPONENTS)
  {
    build_symmetric = 1;
  }

  if (strstr(graph_base, "kron") != NULL || strstr(graph_base, "italy") != NULL || strstr(graph_base, "europe") != NULL)
  {
    build_symmetric = 1;
  }

  for (int index = 3; index < argc; ++index)
  {
    if (strcmp(argv[index], "--symmetric") == 0 || strcmp(argv[index], "-s") == 0)
    {
      build_symmetric = 1;
    }
  }

  CXL_Graph* graph = read_graph_serial(graph_file, build_symmetric);
  if (!graph)
  {
    return EXIT_FAILURE;
  }

  VProp* props = (VProp*)calloc(graph->num_vertices, sizeof(VProp));
  if (!props)
  {
    destroy_graph(graph);
    return EXIT_FAILURE;
  }

  clock_t start = clock();
  size_t iterations = 0;

  switch (algorithm)
  {
    case ALGO_PAGE_RANK:
      printf("Running serial PageRank on %u vertices and %zu edges\n", graph->num_vertices, graph->num_edges);
      iterations = run_pagerank(graph, props);
      break;
    case ALGO_CONNECTED_COMPONENTS:
      printf("Running serial Connected Components on %u vertices and %zu edges\n", graph->num_vertices, graph->num_edges);
      iterations = run_connected_components(graph, props);
      break;
    case ALGO_SSSP:
      printf("Running serial SSSP on %u vertices and %zu edges\n", graph->num_vertices, graph->num_edges);
      iterations = run_sssp(graph, props);
      break;
    default:
      fprintf(stderr, "Unsupported algorithm: %s\n", algorithm_name);
      free(props);
      destroy_graph(graph);
      return EXIT_FAILURE;
  }

  clock_t end = clock();
  double elapsed_seconds = (double)(end - start) / (double)CLOCKS_PER_SEC;
  printf("Completed in %zu iterations, %.3f seconds\n", iterations, elapsed_seconds);

  free(props);
  destroy_graph(graph);
  return EXIT_SUCCESS;
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "graph_io.h"

static CXL_Graph* allocate_graph(void)
{
  CXL_Graph* graph = (CXL_Graph*)calloc(1, sizeof(CXL_Graph));
  if (!graph)
  {
    perror("calloc");
  }
  return graph;
}

static void build_symmetric_csr(CXL_Graph* graph, size_t num_vertices, size_t edge_count)
{
  size_t sym_edges = edge_count * 2U;
  size_t* degree = (size_t*)calloc(num_vertices, sizeof(size_t));
  if (!degree)
  {
    return;
  }

  for (vid_t src = 0; src < (vid_t)num_vertices; ++src)
  {
    for (size_t edge = graph->row_ptr[src]; edge < graph->row_ptr[src + 1]; ++edge)
    {
      vid_t dst = graph->col_idx[edge];
      degree[src]++;
      degree[dst]++;
    }
  }

  graph->row_ptr_sym = (size_t*)calloc(num_vertices + 1U, sizeof(size_t));
  graph->col_idx_sym = (vid_t*)calloc(sym_edges, sizeof(vid_t));
  if (!graph->row_ptr_sym || !graph->col_idx_sym)
  {
    free(degree);
    return;
  }

  for (vid_t vertex = 1; vertex <= (vid_t)num_vertices; ++vertex)
  {
    graph->row_ptr_sym[vertex] = graph->row_ptr_sym[vertex - 1] + degree[vertex - 1];
  }

  size_t* next_pos = (size_t*)malloc(num_vertices * sizeof(size_t));
  if (!next_pos)
  {
    perror("malloc");
    free(degree);
    return;
  }

  memcpy(next_pos, graph->row_ptr_sym, num_vertices * sizeof(size_t));
  for (vid_t src = 0; src < (vid_t)num_vertices; ++src)
  {
    for (size_t edge = graph->row_ptr[src]; edge < graph->row_ptr[src + 1]; ++edge)
    {
      vid_t dst = graph->col_idx[edge];
      graph->col_idx_sym[next_pos[src]++] = dst;
      graph->col_idx_sym[next_pos[dst]++] = src;
    }
  }

  free(next_pos);
  free(degree);
  graph->is_symmetric = 1;
}

CXL_Graph* read_graph_serial(const char* filename, int build_symmetric)
{
  FILE* file = fopen(filename, "r");
  if (!file)
  {
    perror("fopen");
    return NULL;
  }

  char line[512];
  while (fgets(line, sizeof(line), file))
  {
    if (line[0] != '%')
    {
      break;
    }
  }

  vid_t num_vertices = 0;
  vid_t num_cols = 0;
  size_t num_edges = 0;
  if (sscanf(line, "%u %u %zu", &num_vertices, &num_cols, &num_edges) != 3)
  {
    fprintf(stderr, "Failed to parse MatrixMarket header in %s\n", filename);
    fclose(file);
    return NULL;
  }

  if (num_vertices != num_cols)
  {
    fprintf(stderr, "Warning: non-square graph header (%u x %u)\n", num_vertices, num_cols);
  }

  CXL_Graph* graph = allocate_graph();
  if (!graph)
  {
    fclose(file);
    return NULL;
  }

  graph->num_vertices = num_vertices;
  graph->num_edges = num_edges;
  graph->row_ptr = (size_t*)calloc(num_vertices + 1U, sizeof(size_t));
  graph->col_idx = (vid_t*)calloc(num_edges, sizeof(vid_t));
  graph->out_degree = (int*)calloc(num_vertices, sizeof(int));
  if (!graph->row_ptr || !graph->col_idx || !graph->out_degree)
  {
    fclose(file);
    destroy_graph(graph);
    return NULL;
  }

  vid_t* sources = (vid_t*)malloc(num_edges * sizeof(vid_t));
  vid_t* destinations = (vid_t*)malloc(num_edges * sizeof(vid_t));
  if (!sources || !destinations)
  {
    perror("malloc");
    free(sources);
    free(destinations);
    fclose(file);
    destroy_graph(graph);
    return NULL;
  }

  size_t edge_count = 0;
    vid_t min_idx = (vid_t)(-1);

    while (fgets(line, sizeof(line), file) && edge_count < num_edges)
    {
      vid_t src = 0;
      vid_t dst = 0;
      if (sscanf(line, "%u %u", &src, &dst) != 2)
      {
        continue;
      }

      if (src < min_idx) min_idx = src;
      if (dst < min_idx) min_idx = dst;

      sources[edge_count] = src;
      destinations[edge_count] = dst;
      edge_count++;
    }

    fclose(file);

    vid_t offset = (min_idx == 1) ? 1 : 0;
    size_t valid_edges = 0;

    for (size_t i = 0; i < edge_count; ++i)
    {
      vid_t src = sources[i] - offset;
      vid_t dst = destinations[i] - offset;

      if (src >= num_vertices || dst >= num_vertices)
      {
        fprintf(stderr, "Skipping out-of-bounds edge %u -> %u\n", src, dst);
        continue;
      }

      sources[valid_edges] = src;
      destinations[valid_edges] = dst;
      graph->row_ptr[src + 1]++;
      graph->out_degree[src]++;
      valid_edges++;
    }

    edge_count = valid_edges;
    graph->num_edges = valid_edges;
  for (vid_t vertex = 1; vertex <= num_vertices; ++vertex)
  {
    graph->row_ptr[vertex] += graph->row_ptr[vertex - 1];
  }

  size_t* next_pos = (size_t*)malloc(num_vertices * sizeof(size_t));
  if (!next_pos)
  {
    perror("malloc");
    free(sources);
    free(destinations);
    destroy_graph(graph);
    return NULL;
  }

  memcpy(next_pos, graph->row_ptr, num_vertices * sizeof(size_t));
  for (size_t edge = 0; edge < edge_count; ++edge)
  {
    vid_t src = sources[edge];
    graph->col_idx[next_pos[src]++] = destinations[edge];
  }

  free(next_pos);
  free(sources);
  free(destinations);

  if (build_symmetric)
  {
    build_symmetric_csr(graph, num_vertices, edge_count);
  }

  return graph;
}

void destroy_graph(CXL_Graph* graph)
{
  if (!graph)
  {
    return;
  }

  free(graph->row_ptr);
  free(graph->col_idx);
  free(graph->out_degree);

  if (graph->row_ptr_sym && graph->row_ptr_sym != graph->row_ptr)
  {
    free(graph->row_ptr_sym);
  }
  if (graph->col_idx_sym && graph->col_idx_sym != graph->col_idx)
  {
    free(graph->col_idx_sym);
  }

  free(graph);
}

const size_t* graph_row_ptr(const CXL_Graph* graph, int use_symmetric)
{
  if (use_symmetric && graph->is_symmetric && graph->row_ptr_sym)
  {
    return graph->row_ptr_sym;
  }
  return graph->row_ptr;
}

const vid_t* graph_col_idx(const CXL_Graph* graph, int use_symmetric)
{
  if (use_symmetric && graph->is_symmetric && graph->col_idx_sym)
  {
    return graph->col_idx_sym;
  }
  return graph->col_idx;
}

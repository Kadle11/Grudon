#include "../include-c/host.h"

CXL_Graph* read_graph(const char* filename)
{
  FILE* file = fopen(filename, "r");
  if (!file)
  {
    perror("Failed to open graph file");
    return NULL;
  }

  // Skip comments and read header
  char line[256];
  while (fgets(line, sizeof(line), file))
  {
    if (line[0] == '%')
      continue;  // Skip comment lines
    break;       // Header line found
  }

  vid_t num_vertices;
  vid_t num_cols;
  size_t num_edges;
  sscanf(line, "%u %u %zu", &num_vertices, &num_cols, &num_edges);
  printf("Graph header: %u vertices, %zu edges\n", num_vertices, num_edges);
  if (num_vertices != num_cols)
  {
    printf("Warning: non-square matrix header detected (%u x %u)\n", num_vertices, num_cols);
  }
  // Allocate CXL_Graph structure
  CXL_Graph* graph = (CXL_Graph*)cxl_malloc(sizeof(CXL_Graph));
  graph->num_vertices = num_vertices;
  graph->num_edges = num_edges;
  printf("Allocating CXL graph structures...\n");
  graph->row_ptr = (size_t*)cxl_calloc(num_vertices + 1, sizeof(size_t));
  graph->col_idx = (vid_t*)cxl_malloc(num_edges * sizeof(vid_t));
  graph->out_degree = (int*)cxl_calloc(num_vertices + 1, sizeof(int));
  graph->row_ptr_sym = NULL;
  graph->col_idx_sym = NULL;
  graph->is_symmetric = 0;

  vid_t* srcs = (vid_t*)malloc(num_edges * sizeof(vid_t));
  vid_t* dsts = (vid_t*)malloc(num_edges * sizeof(vid_t));

  // Read edges and populate CSR structure
  size_t edge_idx = 0;
  printf("Reading edges and populating CSR structure...\n");
  while (fgets(line, sizeof(line), file) && edge_idx < num_edges)
  {
    vid_t src, dst;
    if (sscanf(line, "%u %u", &src, &dst) != 2)
      continue;

    /* Convert from 1-based MatrixMarket indices to 0-based internal indices. */
    if (src == 0 || dst == 0)
    {
      /* If file already contains 0-based indices, keep them as-is. */
      /* Nothing to do */
    }
    else
    {
      src -= 1;
      dst -= 1;
    }

    if (src >= num_vertices || dst >= num_vertices)
    {
      fprintf(stderr, "Edge indices out of bounds: %u -> %u (n=%u)\n", src, dst, num_vertices);
      continue;
    }

    srcs[edge_idx] = src;
    dsts[edge_idx] = dst;
    graph->out_degree[src]++;
    graph->row_ptr[src + 1]++;
    edge_idx++;
  }
  printf("Finished reading edges. Total edges read: %zu\n", edge_idx);
  fclose(file);

  for (vid_t v = 1; v <= num_vertices; ++v)
  {
    graph->row_ptr[v] += graph->row_ptr[v - 1];
  }

  size_t* next_pos = (size_t*)malloc(num_vertices * sizeof(size_t));
  memcpy(next_pos, graph->row_ptr, num_vertices * sizeof(size_t));
  for (size_t e = 0; e < edge_idx; ++e)
  {
    vid_t src = srcs[e];
    size_t pos = next_pos[src]++;
    graph->col_idx[pos] = dsts[e];
  }

  free(srcs);
  free(dsts);
  free(next_pos);

  cxl_flush_range(graph->row_ptr, (num_vertices + 1) * sizeof(size_t));
  cxl_flush_range(graph->col_idx, num_edges * sizeof(vid_t));
  cxl_flush_range(graph->out_degree, (num_vertices + 1) * sizeof(int));

  // If filename already indicates symmetric (.sgr) we can treat forward CSR as symmetric.
  const char* dot = strrchr(filename, '.');
  if (dot && strcmp(dot, ".sgr") == 0)
  {
    graph->row_ptr_sym = graph->row_ptr;
    graph->col_idx_sym = graph->col_idx;
    graph->is_symmetric = 1;
    return graph;
  }

  // Otherwise build symmetric CSR (duplicate edges in reverse).
  {
    // compute symmetric degrees
    size_t sym_edges = edge_idx * 2;
    size_t* deg_sym = (size_t*)calloc(num_vertices + 1, sizeof(size_t));
    // srcs and dsts are freed above; re-read edges from graph->col_idx by scanning forward CSR
    for (vid_t u = 0; u < (vid_t)num_vertices; ++u)
    {
      for (size_t p = graph->row_ptr[u]; p < graph->row_ptr[u + 1]; ++p)
      {
        vid_t v = graph->col_idx[p];
        deg_sym[u]++;
        deg_sym[v]++;
      }
    }

    // allocate symmetric CSR
    graph->row_ptr_sym = (size_t*)cxl_calloc(num_vertices + 1, sizeof(size_t));
    graph->col_idx_sym = (vid_t*)cxl_malloc(sym_edges * sizeof(vid_t));

    // prefix sum
    for (vid_t v = 1; v <= (vid_t)num_vertices; ++v)
    {
      graph->row_ptr_sym[v] = graph->row_ptr_sym[v - 1] + deg_sym[v - 1];
    }

    // temporary next positions
    size_t* next_sym = (size_t*)malloc(num_vertices * sizeof(size_t));
    memcpy(next_sym, graph->row_ptr_sym, num_vertices * sizeof(size_t));

    for (vid_t u = 0; u < (vid_t)num_vertices; ++u)
    {
      for (size_t p = graph->row_ptr[u]; p < graph->row_ptr[u + 1]; ++p)
      {
        vid_t v = graph->col_idx[p];
        size_t pos_uv = next_sym[u]++;
        graph->col_idx_sym[pos_uv] = v;
        size_t pos_vu = next_sym[v]++;
        graph->col_idx_sym[pos_vu] = u;
      }
    }

    free(next_sym);
    free(deg_sym);

    cxl_flush_range(graph->row_ptr_sym, (num_vertices + 1) * sizeof(size_t));
    cxl_flush_range(graph->col_idx_sym, sym_edges * sizeof(vid_t));

    graph->is_symmetric = 1;
  }

  return graph;
}

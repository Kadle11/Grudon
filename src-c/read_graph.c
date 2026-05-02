#include "../include-c/host.h"

CXL_Graph* read_graph(const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open graph file");
        return NULL;
    }

    // Skip comments and read header
    char line[256];
    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '%') continue; // Skip comment lines
        break; // Header line found
    }

    vid_t num_vertices;
    vid_t num_cols;
    size_t num_edges;
    sscanf(line, "%u %u %zu", &num_vertices, &num_cols, &num_edges);
    printf("Graph header: %u vertices, %zu edges\n", num_vertices, num_edges);
    if (num_vertices != num_cols) {
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

    vid_t* srcs = (vid_t*)malloc(num_edges * sizeof(vid_t));
    vid_t* dsts = (vid_t*)malloc(num_edges * sizeof(vid_t));

    // Read edges and populate CSR structure
    size_t edge_idx = 0;
    printf("Reading edges and populating CSR structure...\n");
    while (fgets(line, sizeof(line), file) && edge_idx < num_edges) {
        vid_t src, dst;
        sscanf(line, "%u %u", &src, &dst);
        srcs[edge_idx] = src;
        dsts[edge_idx] = dst;
        graph->out_degree[src]++;
        graph->row_ptr[src + 1]++;
        edge_idx++;
    }
    printf("Finished reading edges. Total edges read: %zu\n", edge_idx);
    fclose(file);

    for (vid_t v = 1; v <= num_vertices; ++v) {
        graph->row_ptr[v] += graph->row_ptr[v - 1];
    }

    size_t* next_pos = (size_t*)malloc(num_vertices * sizeof(size_t));
    memcpy(next_pos, graph->row_ptr, num_vertices * sizeof(size_t));
    for (size_t e = 0; e < edge_idx; ++e) {
        vid_t src = srcs[e];
        size_t pos = next_pos[src]++;
        graph->col_idx[pos] = dsts[e];
    }

    free(srcs);
    free(dsts);
    free(next_pos);

    cxl_flush(graph->row_ptr, (num_vertices + 1) * sizeof(size_t));
    cxl_flush(graph->col_idx, num_edges * sizeof(vid_t));
    cxl_flush(graph->out_degree, (num_vertices + 1) * sizeof(int));

    return graph;
}

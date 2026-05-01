#include "../include-c/host.h"

CXL_Graph* read_graph_from_mtx_to_cxl(const char* filename) {
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

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <graph_file.mtx>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char* graph_file = argv[1];
    CXL_Graph* graph = read_graph_from_mtx_to_cxl(graph_file);
    if (!graph) {
        return EXIT_FAILURE;
    }

    printf("Graph loaded: %u vertices, %zu edges\n", graph->num_vertices, graph->num_edges);

    // Setup initial vertex properties and frontier (for simplicity, we start with all vertices in the frontier)
    VProp* vprop_masters = (VProp*)calloc(graph->num_vertices, sizeof(VProp));
    uint32_t* frontier = (uint32_t*)calloc((graph->num_vertices + 31) / 32, sizeof(uint32_t));
    VProp* vprops = (VProp*)cxl_malloc(graph->num_vertices * sizeof(VProp));
    uint32_t* frontier_ndp = (uint32_t*)cxl_malloc(((graph->num_vertices + 31) / 32) * sizeof(uint32_t));
    for (vid_t i = 0; i < graph->num_vertices; i++) {
        set_bit(frontier, i); // Add all vertices to the initial frontier
        if (graph->out_degree[i] > 0) {
            vprop_masters[i].pr = DAMPING_FACTOR * (1.0f - DAMPING_FACTOR) / (float)graph->out_degree[i];
        } else {
            vprop_masters[i].pr = 0.0f;
        }
        vprop_masters[i].delta = 0.0f;
        vprop_masters[i].update_sum = 0.0f;
    }
    cxl_memcpy_to_device(vprops, vprop_masters, graph->num_vertices * sizeof(VProp));
    cxl_memcpy_to_device(frontier_ndp, frontier, ((graph->num_vertices + 31) / 32) * sizeof(uint32_t));
    cxl_flush(vprops, graph->num_vertices * sizeof(VProp));
    cxl_flush(frontier_ndp, ((graph->num_vertices + 31) / 32) * sizeof(uint32_t));

    int iteration = 0;
    while (iteration < MAX_ITERATIONS) {
        printf("Iteration %d: Offloading PageRank update generation to CXL...\n", iteration);
        size_t mask_size = ((graph->num_vertices + 31) / 32);
        uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));
        uint32_t* next_frontier_ndp = (uint32_t*)calloc(mask_size, sizeof(uint32_t));

        command_entry_t cmd = {
            .cid = iteration,
            .opcode = OPCODE_GEN_UPDATES,
            .num_vertices = graph->num_vertices,
            .frontier_ndp = frontier_ndp,
            .vprops_mirror = vprops,
            .graph = graph
        };
        cxl_send_cmd(&cmd);
        cxl_wait_done(cmd.cid);
        printf("Iteration %d: NDP job completed. Processing updates on host...\n", iteration);

        // Read back updated vertex properties from CXL memory
        cxl_memcpy_to_host(vprop_masters, vprops, graph->num_vertices * sizeof(VProp));
        int any_set = 0;
        int active_count = 0;

        for (vid_t i = 0; i < graph->num_vertices; ++i) {
            float update_val = vprop_masters[i].delta;
            if (update_val > THRESHOLD) {
                set_bit(next_frontier, i);
                set_bit(next_frontier_ndp, i);
                vprop_masters[i].update_sum = update_val;
                if (graph->out_degree[i] > 0) {
                    vprop_masters[i].pr = DAMPING_FACTOR * update_val / (float)graph->out_degree[i];
                } else {
                    vprop_masters[i].pr = 0.0f;
                }
                any_set = 1;
                active_count++;
            }

            // Consume the update so it isn't reprocessed next iteration.
            vprop_masters[i].delta = 0.0f;
        }

        // Copy updated frontier and vprops back to CXL memory for the next NDP job
        memcpy(frontier, next_frontier, mask_size * sizeof(uint32_t));
        memcpy(frontier_ndp, next_frontier_ndp, mask_size * sizeof(uint32_t));
        cxl_memcpy_to_device(frontier_ndp, frontier, mask_size * sizeof(uint32_t));
        cxl_memcpy_to_device(vprops, vprop_masters, graph->num_vertices * sizeof(VProp));

        free(next_frontier);
        free(next_frontier_ndp);

        // Break early if no vertices are active
        if (!any_set) {
            printf("No active vertices remaining, terminating at iteration %d\n", iteration);
            break;
        } else {
            printf("Active vertices remain %d, proceeding to iteration %d\n", active_count, iteration + 1);
        }

        iteration++;
    }

    // Clean up
    cxl_free(graph->row_ptr);
    cxl_free(graph->col_idx);
    cxl_free(graph->out_degree);
    cxl_free(graph);

    return EXIT_SUCCESS;
}




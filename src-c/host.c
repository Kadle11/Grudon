#include "../include-c/host.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <graph_file.mtx>\n", argv[0]);
        return EXIT_FAILURE;
    }

    //////////////////////////////////////////////////////////////////////////////
    ///
    ///  Step 1: Initialization
    ///
    /////////////////////////////////////////////////////////////////////////////

    // Setting up CXL Graph
    const char* graph_file = argv[1];
    CXL_Graph* graph = read_graph(graph_file);

    if (!graph) {
        return EXIT_FAILURE;
    }

    printf("Graph loaded: %u vertices, %zu edges\n", graph->num_vertices, graph->num_edges);

    // For simplicity, all vertices on frontier
    
    // Host Allocation
    VProp* vprop_masters = (VProp*)calloc(graph->num_vertices, sizeof(VProp));
    uint32_t* frontier_host = (uint32_t*)calloc((graph->num_vertices + 31) / 32, sizeof(uint32_t));

    // CXL Allocation
    VProp* vprop_mirrors = (VProp*)cxl_malloc(graph->num_vertices * sizeof(VProp));
    uint32_t* frontier_ndp = (uint32_t*)cxl_malloc(((graph->num_vertices + 31) / 32) * sizeof(uint32_t));

    for (vid_t i = 0; i < graph->num_vertices; i++) {
        set_bit(frontier_host, i); // Add all vertices to the initial frontier
        if (graph->out_degree[i] > 0) {
            vprop_masters[i].pr = DAMPING_FACTOR * (1.0F - DAMPING_FACTOR) / (float)graph->out_degree[i];
        } else {
            vprop_masters[i].pr = 0.0F;
        }
        vprop_masters[i].delta = 0.0F;
        vprop_masters[i].update_sum = 0.0F;
    }

    cxl_memcpy_to_device(vprop_mirrors, vprop_masters, graph->num_vertices * sizeof(VProp));
    cxl_memcpy_to_device(frontier_ndp, frontier_host, ((graph->num_vertices + 31) / 32) * sizeof(uint32_t));

    cxl_flush(vprop_mirrors, graph->num_vertices * sizeof(VProp));
    cxl_flush(frontier_ndp, ((graph->num_vertices + 31) / 32) * sizeof(uint32_t));


    //////////////////////////////////////////////////////////////////////////////
    ///
    ///  Step 2: Run Loop
    ///
    /////////////////////////////////////////////////////////////////////////////


    int iteration = 0;
    while (iteration < MAX_ITERATIONS) {
        printf("Iteration %d: Offloading PageRank update generation to CXL...\n", iteration);
        size_t mask_size = ((graph->num_vertices + 31) / 32);
        uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));

        command_entry_t cmd = {
            .cid = iteration,
            .opcode = OPCODE_GEN_UPDATES,
            .num_vertices = graph->num_vertices,
            .frontier_ndp = frontier_ndp,
            .vprops_mirror = vprop_mirrors,
            .graph = graph
        };

        cxl_send_cmd(&cmd);
        cxl_wait_done(cmd.cid);
        printf("Iteration %d: NDP job completed. Processing updates on host...\n", iteration);

        // Read back updated vertex properties from CXL memory
        cxl_memcpy_to_host(vprop_masters, vprop_mirrors, graph->num_vertices * sizeof(VProp));
        int any_set = 0;
        int active_count = 0;

        for (vid_t i = 0; i < graph->num_vertices; ++i) {
            float update_val = vprop_masters[i].delta;
            if (update_val > THRESHOLD) {
                set_bit(next_frontier, i);
                vprop_masters[i].update_sum = update_val;
                if (graph->out_degree[i] > 0) {
                    vprop_masters[i].pr = DAMPING_FACTOR * update_val / (float)graph->out_degree[i];
                } else {
                    vprop_masters[i].pr = 0.0F;
                }
                any_set = 1;
                active_count++;
            }

            // Consume the update so it isn't reprocessed next iteration.
            vprop_masters[i].delta = 0.0F;
        }

        // Copy updated frontier and vprops back to CXL memory for the next NDP job
        memcpy(frontier_host, next_frontier, mask_size * sizeof(uint32_t));
        cxl_memcpy_to_device(frontier_ndp, frontier_host, mask_size * sizeof(uint32_t));
        cxl_memcpy_to_device(vprop_mirrors, vprop_masters, graph->num_vertices * sizeof(VProp));

        free(next_frontier);

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


#include "../include-c/host.h"

static inline void swap_rank_pair(RankPair* a, RankPair* b) {
    RankPair tmp = *a;
    *a = *b;
    *b = tmp;
}

static inline int compare_rank_pair_desc(const RankPair* a, const RankPair* b) {
    return (a->score < b->score) ? 1 : (a->score > b->score) ? -1 : 0;
}

static size_t partition_rank_pairs(RankPair* arr, size_t left, size_t right) {
    size_t mid = left + (right - left) / 2;
    if (compare_rank_pair_desc(&arr[mid], &arr[left]) < 0) {
        swap_rank_pair(&arr[mid], &arr[left]);
    }
    if (compare_rank_pair_desc(&arr[right], &arr[left]) < 0) {
        swap_rank_pair(&arr[right], &arr[left]);
    }
    if (compare_rank_pair_desc(&arr[right], &arr[mid]) < 0) {
        swap_rank_pair(&arr[right], &arr[mid]);
    }

    swap_rank_pair(&arr[mid], &arr[right]);
    RankPair pivot = arr[right];

    size_t i = left;
    for (size_t j = left; j < right; ++j) {
        if (compare_rank_pair_desc(&arr[j], &pivot) < 0) {
            swap_rank_pair(&arr[i], &arr[j]);
            ++i;
        }
    }
    swap_rank_pair(&arr[i], &arr[right]);
    return i;
}

static void insertion_sort_rank_pairs(RankPair* arr, size_t left, size_t right) {
    for (size_t i = left + 1; i <= right; ++i) {
        RankPair key = arr[i];
        size_t j = i;
        while (j > left && compare_rank_pair_desc(&key, &arr[j - 1]) < 0) {
            arr[j] = arr[j - 1];
            --j;
        }
        arr[j] = key;
    }
}

static void quicksort_rank_pairs(RankPair* arr, size_t count) {
    if (count < 2) {
        return;
    }

    const size_t threshold = 24;
    size_t stack_left[64];
    size_t stack_right[64];
    size_t top = 0;

    stack_left[top] = 0;
    stack_right[top] = count - 1;
    ++top;

    while (top > 0) {
        --top;
        size_t left = stack_left[top];
        size_t right = stack_right[top];

        while (right > left) {
            size_t size = right - left + 1;
            if (size <= threshold) {
                insertion_sort_rank_pairs(arr, left, right);
                break;
            }

            size_t pivot = partition_rank_pairs(arr, left, right);
            size_t left_size = (pivot > left) ? (pivot - left) : 0;
            size_t right_size = (right > pivot) ? (right - pivot) : 0;

            if (left_size < right_size) {
                if (pivot + 1 < right) {
                    stack_left[top] = pivot + 1;
                    stack_right[top] = right;
                    ++top;
                }
                if (pivot == 0) {
                    break;
                }
                right = (pivot > 0) ? (pivot - 1) : 0;
            } else {
                if (pivot > 0 && pivot - 1 > left) {
                    stack_left[top] = left;
                    stack_right[top] = pivot - 1;
                    ++top;
                }
                left = pivot + 1;
            }
        }
    }
}

void init_pagerank_galois_style(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier) {
    float init_residual = 1.0f - DAMPING_FACTOR;
    for (vid_t i = 0; i < graph->num_vertices; i++) {
        set_bit(frontier, i); 
        vprop_masters[i].score = 0.0f; // PageRank Score
        
        // This is equivalent to pushing the initial residual immediately
        if (graph->out_degree[i] > 0) {
            vprop_masters[i].pr = DAMPING_FACTOR * init_residual / (float)graph->out_degree[i];
        } else {
            vprop_masters[i].pr = 0.0f;
        }
        vprop_masters[i].score += init_residual;
        vprop_masters[i].delta = 0.0f; 
        vprop_masters[i].update_sum = 0.0f; 
    }
}

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
    
    init_pagerank_galois_style(graph, vprop_masters, frontier_host);

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
                vprop_masters[i].score += update_val; // Track ACTUAL score!
                if (graph->out_degree[i] > 0) {
                    vprop_masters[i].pr = DAMPING_FACTOR * update_val / (float)graph->out_degree[i];
                } else {
                    vprop_masters[i].pr = 0.0F;
                }
                any_set = 1;
                active_count++;
            } else {
                vprop_masters[i].pr = 0.0f; // IMPORTANT: Prevent pushing stale pr again!
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

    // Print top 10 ranked vertices by PageRank score
    printf("\n=== Top 10 Ranked Vertices by PageRank ===\n");
    
    // Create array of (vertex_id, pagerank_score) pairs
    RankPair* ranks = (RankPair*)malloc(graph->num_vertices * sizeof(RankPair));
    for (vid_t i = 0; i < graph->num_vertices; i++) {
        ranks[i].vertex_id = i;
        ranks[i].score = vprop_masters[i].score; // Use tracked actual score
    }
    
    // Sort by score in descending order
    quicksort_rank_pairs(ranks, (size_t)graph->num_vertices);
    
    // Print top 10
    size_t top_count = (graph->num_vertices < 10) ? graph->num_vertices : 10;
    for (size_t i = 0; i < top_count; i++) {
        printf("Rank %zu: Vertex %u with PageRank score = %.6f\n", i + 1, ranks[i].vertex_id, ranks[i].score);
    }
    
    free(ranks);

    // Clean up
    cxl_free(graph->row_ptr);
    cxl_free(graph->col_idx);
    cxl_free(graph->out_degree);
    cxl_free(graph);

    return EXIT_SUCCESS;
}


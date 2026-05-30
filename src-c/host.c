#include "../include-c/host.h"

int main(int argc, char* argv[])
{
  if (argc < 3)
  {
    (void)fprintf(stderr, "Usage: %s <graph_file.mtx> <algorithm> [--symmetric|-s]\n", argv[0]);
    return EXIT_FAILURE;
  }

  //////////////////////////////////////////////////////////////////////////////
  ///
  ///  Step 1: Initialization
  ///
  /////////////////////////////////////////////////////////////////////////////

  // Shared pool setup (size can be tuned as needed).
  const size_t pool_size_bytes = 1024ULL * 1024ULL * 1024ULL; // 1 GiB
  if (cxl_pool_init(pool_size_bytes) != 0)
  {
    fprintf(stderr, "Failed to initialize shared pool\n");
    return EXIT_FAILURE;
  }

  int sv[2];
  if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv) != 0)
  {
    perror("socketpair");
    cxl_pool_shutdown();
    return EXIT_FAILURE;
  }

  pid_t child = fork();
  if (child < 0)
  {
    perror("fork");
    close(sv[0]);
    close(sv[1]);
    cxl_pool_shutdown();
    return EXIT_FAILURE;
  }
  if (child == 0)
  {
    close(sv[0]);
    cpu_set_t cpuset;
    sched_getaffinity(0, sizeof(cpu_set_t), &cpuset);
    CPU_ZERO(&cpuset);
    CPU_SET(3, &cpuset);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
    (void)device_process_main(sv[1], cxl_pool_base_ptr(), cxl_pool_size_bytes());
    close(sv[1]);
    _exit(0);
  }

  close(sv[1]);
  (void)cxl_ipc_init(sv[0], cxl_pool_base_ptr());
  printf("Host PID: %d, Spawned device PID: %d\n", (int)getpid(), (int)child);
  sched_setaffinity(0, sizeof(cpu_set_t), &(cpu_set_t){ .__bits = {1 << 2} }); // Pin host to CPU 2

  // Setting up CXL Graph
  const char* graph_file = argv[1];
  const char* algorithm = argv[2];
  int build_symmetric = (strcmp(algorithm, "cc") == 0);
  for (int i = 3; i < argc; ++i)
  {
    if (strcmp(argv[i], "--symmetric") == 0 || strcmp(argv[i], "-s") == 0)
    {
      build_symmetric = 1;
    }
  }

  CXL_Graph* graph = read_graph(graph_file, build_symmetric);

  if (!graph)
  {
    return EXIT_FAILURE;
  }


  printf("Graph loaded: %u vertices, %zu edges\n", graph->num_vertices, graph->num_edges);
  size_t mask_size = ((graph->num_vertices + 31) / 32);

  // For simplicity, all vertices on frontier

  // Host Allocation
  VProp* vprop_masters = (VProp*)calloc(graph->num_vertices, sizeof(VProp));
  uint32_t* frontier_host = (uint32_t*)calloc(mask_size, sizeof(uint32_t));

  // CXL Allocation
  VProp* vprop_mirrors = (VProp*)cxl_malloc(graph->num_vertices * sizeof(VProp));
  uint32_t* frontier_ndp = (uint32_t*)cxl_malloc((mask_size * sizeof(uint32_t)));

  long start_time = clock();
  FILE* fd_bench = fopen("/tmp/measurement", "w");
  if (fd_bench)  {
    fprintf(fd_bench, "1\n");
    fprintf(fd_bench, "%d,%d\n", child, (int)getpid());
    fclose(fd_bench);
    sleep(1);
  } else {
    fprintf(stderr, "Warning: Could not create benchmark file\n");
  }

  if (strcmp(algorithm, "pr") == 0)
  {
    init_pagerank(graph, vprop_masters, frontier_host);
  }
  else if (strcmp(algorithm, "cc") == 0)
  {
    init_connected_components(graph, vprop_masters, frontier_host);
  }
  else if (strcmp(algorithm, "sssp") == 0)
  {
    init_sssp(graph, vprop_masters, frontier_host);
  }
  else
  {
    fprintf(stderr, "Unsupported algorithm: %s\n", algorithm);
    return EXIT_FAILURE;
  }

  cxl_memcpy_to_device(vprop_mirrors, vprop_masters, graph->num_vertices * sizeof(VProp));
  cxl_memcpy_to_device(frontier_ndp, frontier_host, ((mask_size * sizeof(uint32_t))));

  cxl_flush_range(vprop_mirrors, graph->num_vertices * sizeof(VProp));
  cxl_flush_range(frontier_ndp, (mask_size * sizeof(uint32_t)));

  //////////////////////////////////////////////////////////////////////////////
  ///
  ///  Step 2: Run Loop
  ///
  /////////////////////////////////////////////////////////////////////////////
  int iteration = 0;
  int opcode = 0;
  switch (algorithm[0])
  {
    case 'p': printf("Running PageRank...\n"); opcode = OPCODE_GEN_UPDATES_PR; break;
    case 'c': printf("Running Connected Components...\n"); opcode = OPCODE_GEN_UPDATES_CC; break;
    case 's': printf("Running SSSP...\n"); opcode = OPCODE_GEN_UPDATES_SSSP; break;
    default: fprintf(stderr, "Unsupported algorithm: %s\n", algorithm); return EXIT_FAILURE;
  }
  uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));

  while (iteration < MAX_ITERATIONS)
  {
    command_entry_t cmd = { .cid = iteration,
                            .opcode = opcode,
                            .num_vertices = graph->num_vertices,
                            .frontier_ndp = frontier_ndp,
                            .vprops_mirror = vprop_mirrors,
                            .graph = graph };

    cxl_send_cmd(&cmd);
    cxl_wait_done(cmd.cid);

    // Read back updated vertex properties from CXL memory
    cxl_memcpy_to_host(vprop_masters, vprop_mirrors, graph->num_vertices * sizeof(VProp));
    
    int update_count = 0;
    switch (opcode)
    {
      case OPCODE_GEN_UPDATES_PR:
        update_count = apply_updates_pagerank(graph, vprop_masters, frontier_host);
        break;
      case OPCODE_GEN_UPDATES_CC:
        update_count = apply_updates_connected_components(graph, vprop_masters, frontier_host);
        break;
      case OPCODE_GEN_UPDATES_SSSP:
        update_count = apply_updates_sssp(graph, vprop_masters, frontier_host);
        break;
    }

    cxl_memcpy_to_device(frontier_ndp, frontier_host, mask_size * sizeof(uint32_t));
    cxl_memcpy_to_device(vprop_mirrors, vprop_masters, graph->num_vertices * sizeof(VProp));

    // Break early if no vertices are active
    if (update_count == 0)
    {
      printf("No active vertices remaining, terminating at iteration %d\n", iteration);
      break;
    }

    iteration++;
  }

  long end_time = clock();
  double elapsed_sec = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  FILE* fd_end = fopen("/tmp/measurement", "w");
  if (fd_end)  {
    fprintf(fd_end, "0\n");
    fprintf(fd_end, "%d,%d\n", child, (int)getpid());
    fclose(fd_end);
  } else {
    fprintf(stderr, "Warning: Could not update benchmark file\n");
  }
  printf("Total execution time: %.3f seconds\n", elapsed_sec);
  free(next_frontier);
  if (strcmp(algorithm, "pr") == 0) 
  {
    printf("\n=== Top 10 Ranked Vertices by PageRank ===\n");
    // Create array of (vertex_id, pagerank_score) pairs
    RankPair* ranks = (RankPair*)malloc(graph->num_vertices * sizeof(RankPair));
    for (vid_t i = 0; i < graph->num_vertices; i++)
    {
      ranks[i].vertex_id = i;
      ranks[i].score = vprop_masters[i].score;  // Use tracked actual score
    }

    quicksort(ranks, (size_t)graph->num_vertices);

    size_t top_count = (graph->num_vertices < 10) ? graph->num_vertices : 10;
    for (size_t i = 0; i < top_count; i++)
    {
      printf("Rank %zu: Vertex %u with PageRank score = %.6f\n", i + 1, ranks[i].vertex_id, ranks[i].score);
    }
    free(ranks);

  } else if (strcmp(algorithm, "cc") == 0)
  {
    printf("\n=== No of Connected Components ===\n");
    // Count unique component IDs
    size_t* component_counts = (size_t*)calloc(graph->num_vertices, sizeof(size_t));
    size_t num_components = 0;
    for (vid_t i = 0; i < graph->num_vertices; i++)
    {
      uint32_t comp_id = (uint32_t)vprop_masters[i].score;
      if (component_counts[comp_id] == 0)
      {
        num_components++;
        component_counts[comp_id] = 1;
      }
    }
    printf("Total Connected Components: %zu\n", num_components);
    free(component_counts);
  } else if (strcmp(algorithm, "sssp") == 0)
  {
    printf("\n=== SSSP Results ===\n");
    // Report simple statistics: number of reachable vertices and maximum distance
    size_t reachable = 0;
    float max_dist = 0.0f;
    for (vid_t i = 0; i < graph->num_vertices; i++)
    {
      float d = vprop_masters[i].score;
      if (d < FLT_MAX)
      {
        reachable++;
        if (d > max_dist) max_dist = d;
      }
    }
    printf("Reachable vertices: %zu / %u\n", reachable, graph->num_vertices);
    printf("Max distance (float): %.6f\n", max_dist);
  }
  free(vprop_masters);
  free(frontier_host);

  cxl_free(frontier_ndp);
  cxl_free(vprop_mirrors);
  // Free graph buffers. If symmetric CSR was built separately, free it too.
  if (graph->is_symmetric && graph->row_ptr_sym && graph->col_idx_sym &&
      !(graph->row_ptr_sym == graph->row_ptr && graph->col_idx_sym == graph->col_idx))
  {
    cxl_free(graph->row_ptr_sym);
    cxl_free(graph->col_idx_sym);
  }
  cxl_free(graph->row_ptr);
  cxl_free(graph->col_idx);
  cxl_free(graph->out_degree);
  cxl_free(graph);

  close(sv[0]);
  cxl_pool_shutdown();

  return EXIT_SUCCESS;
}

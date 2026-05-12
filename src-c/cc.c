#include "../include-c/host.h"

/**
 * init_connected_components
 * -----------------------
 * Initialize vertex properties for the Connected Components algorithm.
 *
 * For each vertex:
 *  - mark it active on the initial frontier (all vertices start active)
 *  - set `score` to the vertex id (each vertex initially believes it is its
 *    own component id)
 *  - set `delta` and `update_sum` to INT_MAX which serves as "no pending
 *    update" sentinel for this push-style implementation.
 */
void init_connected_components(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier)
{
  for (vid_t i = 0; i < graph->num_vertices; i++)
  {
    set_bit(frontier, i);
    vprop_masters[i].score = (float)i;  // Use score to track component ID
    vprop_masters[i].delta = (float)INT_MAX;  // Pending min label from neighbors
    vprop_masters[i].update_sum = (float)INT_MAX;
  }
}

/**
 * apply_updates_connected_components
 * ----------------------------------
 * Consume pending updates produced by the NDP/CXL kernel and prepare the
 * next frontier for the host-driven coordination loop.
 *
 * Behavior:
 *  - For each vertex, read `delta` (the pending min-label update). If it is
 *    smaller than the current `score`, update `score` and mark the vertex as
 *    active in the `next_frontier`.
 *  - Reset `delta` back to the sentinel value so the same update is not
 *    applied repeatedly.
 *  - `update_sum` records the accepted update value for potential debugging
 *    or bookkeeping (keeps parity with PageRank code structure).
 *
 * Returns:
 *  - the number of vertices that will be active in the next iteration
 */
int apply_updates_connected_components(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host)
{
  uint32_t mask_size = (graph->num_vertices + 31) / 32;
  uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));
  int update_count = 0;

  for (vid_t i = 0; i < graph->num_vertices; ++i)
  {
    float update_val = vprop_masters[i].delta;
    if (update_val < vprop_masters[i].score)
    {
      set_bit(next_frontier, i);
      vprop_masters[i].update_sum = update_val;
      vprop_masters[i].score = update_val; // Update to smaller component ID
      update_count++;
    }

    /* Consume the update so it isn't reprocessed next iteration. */
    vprop_masters[i].delta = (float)INT_MAX;
  }

  memcpy(frontier_host, next_frontier, mask_size * sizeof(uint32_t));
  free(next_frontier);
  return update_count;
}
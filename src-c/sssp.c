#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include-c/host.h"

/**
 * Initialize SSSP vertex properties.
 * - All vertices start with "infinite" distance (represented by FLT_MAX).
 * - The source vertex (vertex 0) is set to distance 0 and placed on the frontier.
 */
void init_sssp(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier)
{
  for (vid_t i = 0; i < graph->num_vertices; i++)
  {
    // Use `score` to store the current distance, `delta` to store pending updates.
    vprop_masters[i].score = FLT_MAX;
    vprop_masters[i].delta = FLT_MAX;
    vprop_masters[i].pr = 0.0f;
    vprop_masters[i].update_sum = 0.0f;
  }

  // Single-source: vertex 0
  if (graph->num_vertices > 0)
  {
    set_bit(frontier, 0);
    vprop_masters[0].score = 0.0f;
    vprop_masters[0].delta = 0.0f;
  }
}

/**
 * Apply pending SSSP updates produced by the NDP/CXL offloaded kernel.
 * For each vertex, if a pending `delta` is smaller than the current `score`,
 * adopt the smaller distance and mark the vertex active for the next iteration.
 */
int apply_updates_sssp(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host)
{
  uint32_t mask_size = (graph->num_vertices + 31) / 32;
  uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));
  int update_count = 0;

  for (vid_t i = 0; i < graph->num_vertices; ++i)
  {
    float update_val = vprop_masters[i].delta;
    // If the pending update offers a shorter path, accept it.
    if (update_val < vprop_masters[i].score)
    {
      set_bit(next_frontier, i);
      vprop_masters[i].update_sum = update_val;
      vprop_masters[i].score = update_val;
      update_count++;
    }

    // Consume the update so it isn't reprocessed next iteration.
    vprop_masters[i].delta = FLT_MAX;
  }

  memcpy(frontier_host, next_frontier, mask_size * sizeof(uint32_t));
  free(next_frontier);
  return update_count;
}

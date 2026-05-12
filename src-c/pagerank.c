#include "../include-c/host.h"

void init_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier)
{
  float init_residual = 1.0f - DAMPING_FACTOR;
  for (vid_t i = 0; i < graph->num_vertices; i++)
  {
    set_bit(frontier, i);
    vprop_masters[i].score = 0.0f;  // PageRank Score

    // This is equivalent to pushing the initial residual immediately
    if (graph->out_degree[i] > 0)
    {
      vprop_masters[i].pr = DAMPING_FACTOR * init_residual / (float)graph->out_degree[i];
    }
    else
    {
      vprop_masters[i].pr = 0.0f;
    }

    vprop_masters[i].score += init_residual;
    vprop_masters[i].delta = 0.0f;
    vprop_masters[i].update_sum = 0.0f;
  }
}

int apply_updates_pagerank(CXL_Graph* graph, VProp* vprop_masters, uint32_t* frontier_host)
{
  uint32_t mask_size = (graph->num_vertices + 31) / 32;
  uint32_t* next_frontier = (uint32_t*)calloc(mask_size, sizeof(uint32_t));
  int update_count = 0;
  for (vid_t i = 0; i < graph->num_vertices; ++i)
  {
      float update_val = vprop_masters[i].delta;
      if (update_val > THRESHOLD)
      {
        set_bit(next_frontier, i);
        vprop_masters[i].update_sum = update_val;
        vprop_masters[i].score += update_val;
        if (graph->out_degree[i] > 0)
        {
          vprop_masters[i].pr = DAMPING_FACTOR * update_val / (float)graph->out_degree[i];
        }
        else
        {
          vprop_masters[i].pr = 0.0F;
        }
        update_count++;
      }
      else
      {
        vprop_masters[i].pr = 0.0F;
      }

      // Consume the update so it isn't reprocessed next iteration.
      vprop_masters[i].delta = 0.0F;
  }
  memcpy(frontier_host, next_frontier, mask_size * sizeof(uint32_t));
  free(next_frontier);
  return update_count;
}


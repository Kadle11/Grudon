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

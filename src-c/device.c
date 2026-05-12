#include "../include-c/device.h"

#include <stdio.h>
#include <float.h>

void gen_updates_pagerank(const command_entry_t* cmd)
{
  for (vid_t i = 0; i < cmd->num_vertices; ++i)
  {
    if (!get_bit(cmd->frontier_ndp, i))
    {
      continue;
    }

    float contribution = cmd->vprops_mirror[i].pr;
    for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
    {
      cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta += contribution; // indirect access update for vprops_mirror[nbr].delta
    }
  }
}

void gen_updates_connected_components(const command_entry_t* cmd)
{
  for (vid_t i = 0; i < cmd->num_vertices; ++i)
  {
    if (!get_bit(cmd->frontier_ndp, i))
    {
      continue;
    }

    float component_id = cmd->vprops_mirror[i].score;
    // Use symmetric adjacency if available to treat graph as undirected
    if (cmd->graph->is_symmetric && cmd->graph->row_ptr_sym && cmd->graph->col_idx_sym)
    {
      for (vid_t nbr_idx = cmd->graph->row_ptr_sym[i]; nbr_idx < cmd->graph->row_ptr_sym[i + 1]; ++nbr_idx)
      {
        float old_delta = cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta;
        if (component_id < old_delta)
        {
          cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta = component_id; // indirect access update for vprops_mirror[nbr].delta
        }
      }
    }
    else
    {
      for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
      {
        float old_delta = cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta;
        if (component_id < old_delta)
        {
          cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta = component_id; // indirect access update for vprops_mirror[nbr].delta
        }
      }
    }
  }
}


void gen_updates_sssp(const command_entry_t* cmd)
{
  for (vid_t i = 0; i < cmd->num_vertices; ++i)
  {
    if (!get_bit(cmd->frontier_ndp, i))
    {
      continue;
    }

    float dist = cmd->vprops_mirror[i].score;
    if (dist == FLT_MAX)
    {
      continue; // source or unreachable check
    }

    float candidate = dist + 1.0f; // unweighted graph: edge weight = 1

    // Use symmetric adjacency if available to treat graph as undirected
    if (cmd->graph->is_symmetric && cmd->graph->row_ptr_sym && cmd->graph->col_idx_sym)
    {
      for (vid_t nbr_idx = cmd->graph->row_ptr_sym[i]; nbr_idx < cmd->graph->row_ptr_sym[i + 1]; ++nbr_idx)
      {
        float old_delta = cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta;
        if (candidate < old_delta)
        {
          cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta = candidate; // indirect access update for vprops_mirror[nbr].delta
        }
      }
    }
    else
    {
      for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
      {
        float old_delta = cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta;
        if (candidate < old_delta)
        {
          cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta = candidate;
        }
      }
    }
  }
}


void run_ndp_job(const command_entry_t* cmd)
{
  printf("Command CID: %u, Opcode: %d, Num Vertices: %u\n", cmd->cid, cmd->opcode, cmd->num_vertices);
  switch (cmd->opcode)
  {
    // Handles Generate Updates case for Grudon for PageRank
    case OPCODE_GEN_UPDATES_PR:
      gen_updates_pagerank(cmd);
      break;
    case OPCODE_GEN_UPDATES_CC:
      gen_updates_connected_components(cmd);
      break;
    case OPCODE_GEN_UPDATES_SSSP:
      gen_updates_sssp(cmd);
      break;
    default: printf("Unknown opcode: %d\n", cmd->opcode);
  }
}

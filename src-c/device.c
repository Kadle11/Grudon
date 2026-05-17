#include "../include-c/device.h"

// Atomic-min helper for floats using CAS on the uint32_t bit representation
static inline void atomic_min_float(float *addr, float val)
{
  uint32_t *p = (uint32_t *)addr;
  uint32_t old_bits = __atomic_load_n(p, __ATOMIC_RELAXED);
  while (true)
  {
    float old_val;
    memcpy(&old_val, &old_bits, sizeof(old_val));
    if (!(val < old_val))
    {
      break; // no update needed
    }
    uint32_t new_bits;
    memcpy(&new_bits, &val, sizeof(new_bits));
    if (__atomic_compare_exchange_n(p, &old_bits, new_bits, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
    {
      break; // succeeded
    }
    // on failure, old_bits holds the current value; loop and retry
  }
}

// can make this pull based and remove atomic planning on doing that, but keeping push-based for now
void gen_updates_pagerank_push(const command_entry_t* cmd)
{
  #pragma omp parallel for schedule(static, 1024) // Parallelize over vertices with dynamic scheduling
  for (vid_t i = 0; i < cmd->num_vertices; ++i)
  {
    if (!get_bit(cmd->frontier_ndp, i))
    {
      continue;
    }

    float contribution = cmd->vprops_mirror[i].pr;
    for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
    {
      #pragma omp atomic
      cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta += contribution; // indirect access update for vprops_mirror[nbr].delta
    }
  }
}

void gen_updates_connected_components_push(const command_entry_t* cmd)
{
  #pragma omp parallel for schedule(static, 1024) // Parallelize over vertices with dynamic scheduling
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
        atomic_min_float(&cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta, component_id);
      }
    }
    else
    {
      
      for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
      {
        atomic_min_float(&cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta, component_id);
      }
    }
  }
}

void gen_updates_sssp_push(const command_entry_t* cmd)
{
  #pragma omp parallel for schedule(static, 1024)
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
        atomic_min_float(&cmd->vprops_mirror[cmd->graph->col_idx_sym[nbr_idx]].delta, candidate);
      }
    }
    else
    {
      for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx)
      {
        atomic_min_float(&cmd->vprops_mirror[cmd->graph->col_idx[nbr_idx]].delta, candidate);
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
      gen_updates_pagerank_push(cmd);
//      gen_updates_pagerank_pull(cmd);
      break;
    case OPCODE_GEN_UPDATES_CC:
      gen_updates_connected_components_push(cmd);
//      gen_updates_connected_components_pull(cmd);
      break;
    case OPCODE_GEN_UPDATES_SSSP:
      gen_updates_sssp_push(cmd);
//      gen_updates_sssp_pull(cmd);
      break;
    default: printf("Unknown opcode: %d\n", cmd->opcode);
  }
}

int device_process_main(int sockfd, void* pool_base, size_t pool_size)
{
  (void)pool_size;
  if (cxl_ipc_init(sockfd, pool_base) != 0)
  {
    return -1;
  }

  printf("Device PID: %d (parent %d)\n", (int)getpid(), (int)getppid());

  ipc_msg_t msg;
  for (;;)
  {
    if (ipc_recv_msg(&msg) != 0)
    {
      return -1;
    }

    if (msg.type == IPC_MSG_SIGNAL)
    {
      if (msg.payload.sig.signal == SIGTERM || msg.payload.sig.signal == SIGINT)
      {
        break;
      }
      continue;
    }

    if (msg.type != IPC_MSG_CMD)
    {
      continue;
    }

    command_entry_t cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.cid = msg.payload.cmd.cid;
    cmd.opcode = msg.payload.cmd.opcode;
    cmd.num_vertices = msg.payload.cmd.num_vertices;
    cmd.frontier_ndp = (uint32_t*)pool_offset_to_ptr(pool_base, msg.payload.cmd.frontier_off);
    cmd.vprops_mirror = (VProp*)pool_offset_to_ptr(pool_base, msg.payload.cmd.vprops_off);
    cmd.graph = (CXL_Graph*)pool_offset_to_ptr(pool_base, msg.payload.cmd.graph_off);

    run_ndp_job(&cmd);

    ipc_msg_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.type = IPC_MSG_ACK;
    ack.payload.ack.cid = cmd.cid;
    ack.payload.ack.status = 0;
    ipc_send_msg(&ack);
  }

  return 0;
}

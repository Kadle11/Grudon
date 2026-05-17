#include "../include-c/ipc.h"
#include "../include-c/shmem.h"

static int ipc_sockfd = -1;
static void* ipc_pool_base = NULL;

int ipc_init(int sockfd, void* pool_base)
{
  ipc_sockfd = sockfd;
  ipc_pool_base = pool_base;
  return 0;
}

int ipc_send_msg(const ipc_msg_t* msg)
{
  if (ipc_sockfd < 0 || !msg) return -1;
  ssize_t sent = send(ipc_sockfd, msg, sizeof(*msg), MSG_NOSIGNAL);
  if (sent != (ssize_t)sizeof(*msg))
  {
    return -1;
  }
  return 0;
}

int ipc_recv_msg(ipc_msg_t* msg)
{
  if (ipc_sockfd < 0 || !msg) return -1;
  ssize_t recvd = recv(ipc_sockfd, msg, sizeof(*msg), 0);
  if (recvd == 0)
  {
    return -1; // peer closed
  }
  if (recvd != (ssize_t)sizeof(*msg))
  {
    return -1;
  }
  return 0;
}

int send_signal_to_device(int signal)
{
  ipc_msg_t msg;
  memset(&msg, 0, sizeof(msg));
  msg.type = IPC_MSG_SIGNAL;
  msg.payload.sig.signal = signal;
  return ipc_send_msg(&msg);
}

int send_command_to_device(const command_entry_t* cmd)
{
  if (!cmd || !ipc_pool_base) return -1;

  ipc_msg_t msg;
  memset(&msg, 0, sizeof(msg));
  msg.type = IPC_MSG_CMD;
  msg.payload.cmd.cid = cmd->cid;
  msg.payload.cmd.opcode = cmd->opcode;
  msg.payload.cmd.num_vertices = cmd->num_vertices;
  msg.payload.cmd.frontier_off = pool_ptr_to_offset(ipc_pool_base, cmd->frontier_ndp);
  msg.payload.cmd.vprops_off = pool_ptr_to_offset(ipc_pool_base, cmd->vprops_mirror);
  msg.payload.cmd.graph_off = pool_ptr_to_offset(ipc_pool_base, cmd->graph);

  return ipc_send_msg(&msg);
}

int wait_for_device_completion(uint32_t cid)
{
  ipc_msg_t msg;
  for (;;)
  {
    if (ipc_recv_msg(&msg) != 0)
    {
      return -1;
    }
    if (msg.type == IPC_MSG_ACK && msg.payload.ack.cid == cid)
    {
      return msg.payload.ack.status;
    }
    // Ignore unrelated messages.
  }
}

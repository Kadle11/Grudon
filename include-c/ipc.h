#ifndef IPC_H
#define IPC_H

#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "types.h"

typedef enum
{
	IPC_MSG_CMD = 1,
	IPC_MSG_SIGNAL = 2,
	IPC_MSG_ACK = 3
} ipc_msg_type_t;

typedef struct
{
	uint32_t cid;
	int32_t opcode;
	uint32_t num_vertices;
	uint64_t frontier_off;
	uint64_t vprops_off;
	uint64_t graph_off;
} command_ipc_t;

typedef struct
{
	int32_t signal;
} signal_ipc_t;

typedef struct
{
	uint32_t cid;
	int32_t status;
} ack_ipc_t;

typedef struct
{
	uint32_t type;
	uint32_t reserved;
	union
	{
		command_ipc_t cmd;
		signal_ipc_t sig;
		ack_ipc_t ack;
	} payload;
} ipc_msg_t;

// Initialize IPC state with socket fd and shared-pool base.
int ipc_init(int sockfd, void* pool_base);

// Send a simple signal/notification to the device.
int send_signal_to_device(int signal);

// Send a command structure to the device (asynchronous).
int send_command_to_device(const command_entry_t* cmd);

// Block until the device acknowledges completion for command id `cid`.
int wait_for_device_completion(uint32_t cid);

// Low-level IPC message helpers (used by device process).
int ipc_send_msg(const ipc_msg_t* msg);
int ipc_recv_msg(ipc_msg_t* msg);

#endif // IPC_H


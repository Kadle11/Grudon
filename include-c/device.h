#ifndef DEVICE_H
#define DEVICE_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <float.h>
#include <stdbool.h>
#include <omp.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>

#include "../include-c/types.h"
#include "../include-c/shmem.h"
#include "../include-c/ipc.h"


/**
 * Heap Memory Wrapper Operations for CMMs, provided by Machine API
 */
void* cxl_malloc(size_t size);
void* cxl_calloc(size_t count, size_t size);
void cxl_free(void* ptr);

/**
 * Provided Machine CXL/NDP API
 */
void cxl_flush_range(void* ptr, size_t size);
void cxl_send_cmd(const command_entry_t* cmd);
void cxl_wait_done(uint32_t cid);

// Shared pool and IPC initialization helpers.
int cxl_pool_init(size_t size);
void cxl_pool_shutdown(void);
int cxl_ipc_init(int sockfd, void* pool_base);
void* cxl_pool_base_ptr(void);
size_t cxl_pool_size_bytes(void);

/**
 * Simplified `worker_thread` function for serial, single responsibility tasks
 */
void gen_updates_pagerank(const command_entry_t* cmd);
void gen_updates_connected_components(const command_entry_t* cmd);
void gen_updates_sssp(const command_entry_t* cmd);
void run_ndp_job(const command_entry_t* cmd);
int device_process_main(int sockfd, void* pool_base, size_t pool_size);

/**
 * memcpy between Host RAM and CXL Device RAM helper functions
 */
void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size);
void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size);

#endif  // DEVICE_H

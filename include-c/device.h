#ifndef DEVICE_H
#define DEVICE_H

#include <stddef.h>
#include <stdint.h>

#include "../include-c/types.h"

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

/**
 * Simplified `worker_thread` function for serial, single responsibility tasks
 */
void gen_updates_pagerank(const command_entry_t* cmd);
void gen_updates_connected_components(const command_entry_t* cmd);
void gen_updates_sssp(const command_entry_t* cmd);
void run_ndp_job(const command_entry_t* cmd);

/**
 * memcpy between Host RAM and CXL Device RAM helper functions
 */
void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size);
void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size);

#endif  // DEVICE_H

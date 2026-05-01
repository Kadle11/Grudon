#ifndef DEVICE_H
#define DEVICE_H

#include <stddef.h>
#include <stdint.h>
#include "../include-c/types.h"


void* host_malloc(size_t size);
void* host_calloc(size_t count, size_t size);
void host_free(void* ptr);

void* cxl_malloc(size_t size);
void* cxl_calloc(size_t count, size_t size);
void cxl_free(void* ptr);

void cxl_flush(void* ptr, size_t size);
void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size);
void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size);

void cxl_send_cmd(const command_entry_t* cmd);
void cxl_wait_done(uint32_t cid);
void run_ndp_job(const command_entry_t* cmd);

#endif // DEVICE_H
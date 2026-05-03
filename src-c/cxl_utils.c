#include <stdlib.h>
#include <string.h>

#include "../include-c/device.h"

// Simulated CXL Memory
void* cxl_malloc(size_t size)
{
  return malloc(size);
}
void* cxl_calloc(size_t count, size_t size)
{
  return cxl_malloc(count * size);
}
void cxl_free(void* ptr)
{
  free(ptr);
}

// Simulated Machine API Functions
void cxl_flush_range(void* ptr, size_t size)
{
  // No-op
}
void cxl_wait_done(uint32_t cid)
{
  (void)cid;  // No-op
}
void cxl_send_cmd(const command_entry_t* cmd)
{
  // Simulated offload
  run_ndp_job(cmd);
}

void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size)
{
  cxl_flush_range(cxl_dest, size);
  memcpy(cxl_dest, host_src, size);
  cxl_flush_range(cxl_dest, size);
}

void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size)
{
  cxl_flush_range(cxl_src, size);
  memcpy(host_dest, cxl_src, size);
  cxl_flush_range(cxl_src, size);
}

#include "../include-c/device.h"

static void* cxl_pool_base = NULL;
static size_t cxl_pool_size = 0;
static size_t cxl_pool_offset = 0;

// Simulated CXL Memory
void* cxl_malloc(size_t size)
{
  size_t aligned_size = (size + 63) & ~((size_t)63);  // Align to 64 bytes
  if (cxl_pool_base)
  {
    if (cxl_pool_offset + aligned_size > cxl_pool_size)
    {
      return NULL;
    }
    void* ptr = (void*)((char*)cxl_pool_base + cxl_pool_offset);
    cxl_pool_offset += aligned_size;
    return ptr;
  }
  return malloc(aligned_size);
}
void* cxl_calloc(size_t count, size_t size)
{
  size_t total_size = count * size;
  void* ptr = cxl_malloc(total_size);
  if (ptr)
  {
    memset(ptr, 0, total_size);
  }
  return ptr;
}
void cxl_free(void* ptr)
{
  if (!cxl_pool_base)
  {
    free(ptr);
  }
}

// Simulated Machine API Functions
void cxl_flush_range(void* ptr, size_t size)
{
  // No-op in simulation, but could add memory barriers if needed
}
void cxl_wait_done(uint32_t cid)
{
  // Wait for device to acknowledge completion of command `cid`.
  wait_for_device_completion(cid);
}
void cxl_send_cmd(const command_entry_t* cmd)
{
  // Send the command to the simulated device asynchronously.
  send_command_to_device(cmd);
}

int cxl_pool_init(size_t size)
{
  if (size == 0) return -1;
  void* base = NULL;
  if (mmap_shared_memory_pool(size, &base) != 0)
  {
    return -1;
  }
  cxl_pool_base = base;
  cxl_pool_size = size;
  cxl_pool_offset = 0;
  return 0;
}

void cxl_pool_shutdown(void)
{
  if (cxl_pool_base)
  {
    munmap_shared_memory_pool(cxl_pool_base, cxl_pool_size);
  }
  cxl_pool_base = NULL;
  cxl_pool_size = 0;
  cxl_pool_offset = 0;
}

int cxl_ipc_init(int sockfd, void* pool_base)
{
  return ipc_init(sockfd, pool_base);
}

void* cxl_pool_base_ptr(void)
{
  return cxl_pool_base;
}

size_t cxl_pool_size_bytes(void)
{
  return cxl_pool_size;
}

// Host-Device Data Transfer Wrapper
void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size)
{
  cxl_flush_range(cxl_dest, size);
  memcpy(cxl_dest, host_src, size);
  cxl_flush_range(cxl_dest, size);
}

// Device to Host data transfer wrapper
void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size)
{
  cxl_flush_range(cxl_src, size);
  memcpy(host_dest, cxl_src, size);
  cxl_flush_range(cxl_src, size);
}

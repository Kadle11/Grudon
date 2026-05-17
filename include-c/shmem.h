#ifndef SHMEM_H
#define SHMEM_H

#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <sys/mman.h>

#ifndef MAP_ANONYMOUS
#ifdef MAP_ANON
#define MAP_ANONYMOUS MAP_ANON
#else
#define MAP_ANONYMOUS 0x20
#endif
#endif

int mmap_shared_memory_pool(size_t size, void** base_out);
int munmap_shared_memory_pool(void* base, size_t size);

uint64_t pool_ptr_to_offset(void* base, void* ptr);
void* pool_offset_to_ptr(void* base, uint64_t off);
#endif  // SHMEM_H
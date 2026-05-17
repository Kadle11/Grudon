#include "../include-c/shmem.h"

int mmap_shared_memory_pool(size_t size, void** base_out)
{
    void* base = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED)
    {
        perror("mmap");
        return -1;
    }
    if (base_out)
    {
        *base_out = base;
    }
    return 0;
}

int munmap_shared_memory_pool(void* base, size_t size)
{
    if (munmap(base, size) != 0)
    {
        perror("munmap");
        return -1;
    }
    return 0;
}

uint64_t pool_ptr_to_offset(void* base, void* ptr)
{
    if (!base || !ptr) return 0;
    return (uint64_t)((uintptr_t)ptr - (uintptr_t)base);
}

void* pool_offset_to_ptr(void* base, uint64_t off)
{
    if (!base) return NULL;
    return (void*)((uintptr_t)base + (uintptr_t)off);
}
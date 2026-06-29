#include "csr_sweep.h"

// Touch every page of [p, p+bytes) with a read-modify-write of the same
// value, forcing physical backing without altering contents.
void prefault(void *p, size_t bytes) {
    volatile uint8_t *vp = (volatile uint8_t *)p;
    long pagesize = sysconf(_SC_PAGESIZE);
    for (size_t off = 0; off < bytes; off += (size_t)pagesize) {
        vp[off] = vp[off];
    }
    if (bytes > 0) vp[bytes - 1] = vp[bytes - 1];
}

// Cycles-per-nanosecond of the invariant TSC, measured via a busy-wait
// against CLOCK_MONOTONIC. rdtscp is serializing enough for this purpose.
double calibrate_tsc_ghz(void) {
    struct timespec t0, t1;
    unsigned aux;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t c0 = __rdtscp(&aux);
    do {
        clock_gettime(CLOCK_MONOTONIC, &t1);
    } while ((t1.tv_sec - t0.tv_sec) * 1000000000LL + (t1.tv_nsec - t0.tv_nsec) < 100000000LL);
    uint64_t c1 = __rdtscp(&aux);
    double ns = (double)((t1.tv_sec - t0.tv_sec) * 1000000000LL + (t1.tv_nsec - t0.tv_nsec));
    double cycles = (double)(c1 - c0);
    return cycles / ns;
}

// Refuse to allocate past 90% of physical RAM rather than risk the OOM killer
// mid-sweep. Conservatively sums the transient build-phase footprint (edge
// buffer + same-size sort scratch) with the persistent graph arrays, vprop,
// and (pull) the out array, a slight over-estimate since the edge buffers
// are freed before vprop/out are allocated. Returns 1 if the point fits.
int memory_guard_ok(long long V, uint64_t e_nominal, int pull) {
    long phys_pages = sysconf(_SC_PHYS_PAGES);
    long pagesize = sysconf(_SC_PAGESIZE);
    if (phys_pages <= 0 || pagesize <= 0) return 1;
    double total = (double)phys_pages * (double)pagesize;
    double edge_buf = 2.0 * (double)e_nominal * sizeof(uint64_t); // edges + scratch
    double idx = (double)e_nominal * sizeof(uint32_t);
    double ptr = (double)(V + 1) * sizeof(uint32_t);
    double degs = 2.0 * (double)V * sizeof(uint32_t);
    // pull gathers/writes double vprop+out; push scatters into uint32 vprop.
    double vprop = (double)V * (pull ? sizeof(double) : sizeof(uint32_t));
    double out = pull ? (double)V * sizeof(double) : 0.0;
    double peak = edge_buf + idx + ptr + degs + vprop + out;
    if (peak <= 0.9 * total) return 1;
    fprintf(stderr,
            "skipping V=%lld nominal E=%llu: estimated peak %.2f GB "
            "(> 90%% of %.2f GB physical RAM)\n",
            V, (unsigned long long)e_nominal, peak / 1e9, total / 1e9);
    return 0;
}


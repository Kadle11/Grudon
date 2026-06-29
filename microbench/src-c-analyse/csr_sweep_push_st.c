#include "csr_sweep.h"

// Single-threaded push/scatter sweep -- the csr_sweep baseline, the
// write-side counterpart to csc_sweep_pull_mt's gather. For each source v,
// scatter into each out-neighbour: vprop[col_idx[j]]++. Single-threaded by design: the
// push form has a hub write-contention problem under threading (concurrent
// RMW to shared hot slots)
// This file holds ONLY the kernel and its timing primitive. The sweep loop,
// CLI parsing, and usage text are the caller's / a separate file's;
// prefaulting lives in csr_sweep_utils.c. Convert the returned cycles to ns
// with calibrate_tsc_ghz() (also in csr_sweep_utils.c).

// Pin the calling thread to `cpu`. No-op if cpu < 0. Done before the timed
// region so the affinity syscall stays out of the measurement.
static void pin_self(int cpu) {
    if (cpu < 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    sched_setaffinity(0, sizeof(set), &set);
}

// Push/scatter kernel.
void push_sweep_st(const csr_graph_t *g, uint32_t *vprop) {
    const uint32_t *row_ptr = g->row_ptr;
    const uint32_t *col_idx = g->col_idx;
    int64_t V = g->V;
    for (int64_t v = 0; v < V; v++) {
        uint32_t end = row_ptr[v + 1];
        // Can be replaced by an accelerated instruction (AIA_Read1 and update)
        for (uint32_t j = row_ptr[v]; j < end; j++) {
            vprop[col_idx[j]]++;
        }
    }
}

// Time one invocation of push_sweep_st, returning elapsed TSC cycles. Pass
// cpu >= 0 to pin the thread before timing (cpu < 0 skips).
uint64_t time_push_sweep_st(const csr_graph_t *g, uint32_t *vprop, int cpu) {
    pin_self(cpu);

    unsigned aux;
    uint64_t t0 = __rdtscp(&aux);
    push_sweep_st(g, vprop);
    uint64_t t1 = __rdtscp(&aux);
    return t1 - t0;
}

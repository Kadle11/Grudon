#ifndef CSR_SWEEP_H
#define CSR_SWEEP_H

#define _GNU_SOURCE
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>
#include <x86intrin.h>

typedef enum { DIST_URAND, DIST_KRON } dist_t;

typedef struct {
    dist_t distribution;
    int64_t V;
    uint64_t actual_E; // Actual number of edges removing duplicates and self-loops
    
    uint32_t *row_ptr;   // uint32_t* V+1 entries (keeping generated values below UINT32_MAX, if exceeded, use uint64_t* row_ptr_u64)
    uint32_t *col_idx;  // actual_E entries
    uint32_t *outdeg;   // V entries
    uint32_t *indeg;   // V entries
} csr_graph_t;

typedef struct {
    dist_t distribution;
    int64_t V;
    uint64_t actual_E; // Actual number of edges removing duplicates and self-loops

    uint32_t *col_ptr;   // uint32_t* V+1 entries (keeping generated values below UINT32_MAX, if exceeded, use uint64_t* col_ptr_u64)
    uint32_t *row_idx;  // actual_E entries
    uint32_t *outdeg;   // V entries
    uint32_t *indeg;   // V entries
} csc_graph_t;

// Generate V*avg_degree `dist` edges and build the graph. rng_state is the
// splitmix64 state, advanced in place. Free with the matching free_* helper.
csr_graph_t build_csr(int64_t V, uint64_t avg_degree, bool undirected,
                      dist_t dist, uint64_t *rng_state);
csc_graph_t build_csc(int64_t V, uint64_t avg_degree, bool undirected,
                      dist_t dist, uint64_t *rng_state);
void free_csr_graph(csr_graph_t *g);
void free_csc_graph(csc_graph_t *g);

// Single-threaded push/scatter kernel (csr_sweep_push_st.c): vprop[col_idx[j]]++
// over every out-edge. cpu >= 0 pins the thread; cpu < 0 skips. time_* returns
// TSC cycles.
void push_sweep_st(const csr_graph_t *g, uint32_t *vprop);
uint64_t time_push_sweep_st(const csr_graph_t *g, uint32_t *vprop, int cpu);

// Parallel pull/gather kernel (csc_sweep_pull_mt.c): out[v] = sum of vprop
// over v's in-neighbours. vprop/out are double (8-byte gather element, matching
// real double-precision pull workloads). cpus[] (length num_threads) pins each
// thread to a distinct physical core; NULL skips pinning. time_* returns TSC
// cycles.
void pull_sweep_mt(const csc_graph_t *g, const double *vprop,
                   double *out, int num_threads);
uint64_t time_pull_sweep_mt(const csc_graph_t *g, const double *vprop,
                            double *out, int num_threads, const int *cpus);

// CSV output (csv_writer.c). Unified schema across kernels: `kernel` is
// "push"/"pull" and `threads` is the team size (1 for push); ns_per_access is
// ns/actual_E (one random access per edge for both scatter and gather).
const char *dist_name(dist_t dist);
void csv_write_header(FILE *out);
void csv_write_row(FILE *out, int64_t V, long long avg_degree, dist_t dist,
                   const char *kernel, int threads, uint64_t actual_E,
                   long long run_idx, uint64_t seed, uint64_t cycles,
                   double ns, double ns_per_access);

// Measurement utilities (csr_sweep_utils.c).
void prefault(void *p, size_t bytes);
double calibrate_tsc_ghz(void);
// Returns 1 if one sweep point at (V, e_nominal) fits in ~90% of RAM, else
// logs to stderr and returns 0. `pull` adds the gather out[] array (V*8).
int memory_guard_ok(long long V, uint64_t e_nominal, int pull);
#endif // CSR_SWEEP_H

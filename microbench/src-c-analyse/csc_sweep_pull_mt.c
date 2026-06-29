#include "csr_sweep.h"

#ifdef _OPENMP
#include <omp.h>
#endif

// Parallel pull/gather sweep 
// This file holds ONLY the kernel and its timing primitive. The per-(V,
// avg_degree, dist, threads) sweep loop that drives them, CLI parsing, and
// usage text are the caller's / a separate file's; prefaulting lives in
// csr_sweep_utils.c.

// Pin each thread of a `num_threads`-wide OpenMP team to the core named in
// cpus[] (thread i -> cpus[i]). The caller supplies the exact mapping so it
// can pick distinct *physical* cores and avoid putting two threads on sibling
// hyperthreads of one core (which would share execution/memory ports and
// throttle this bandwidth-bound gather). No-op if cpus is NULL or built
// without OpenMP. 
static void pin_team(int num_threads, const int *cpus) {
    if (!cpus) return;
#ifdef _OPENMP
    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpus[tid], &set);
        sched_setaffinity(0, sizeof(set), &set);
    }
#else
    (void)num_threads;
#endif
}

// Pull/gather kernel. For each destination vertex v, accumulate vprop over
// its in-neighbours (the sources held in g->row_idx, grouped by g->col_ptr)
// and store the total in out[v]. 
void pull_sweep_mt(const csc_graph_t *g, const double *vprop,
                   double *out, int num_threads) {
    const uint32_t *col_ptr = g->col_ptr;
    const uint32_t *row_idx = g->row_idx;
    int64_t V = g->V;
#ifndef _OPENMP
    (void)num_threads; // serial fallback: no team to size
#endif
    #pragma omp parallel for num_threads(num_threads) schedule(dynamic, 1024)
    for (int64_t v = 0; v < V; v++) {
        double sum = 0.0;
        uint32_t end = col_ptr[v + 1];
        // Can be replaced by an accelerated instruction (AIA_Read1)
        for (uint32_t j = col_ptr[v]; j < end; j++) {
            sum += vprop[row_idx[j]];
        }
        out[v] = sum;
    }
}

// Time one pinned invocation of pull_sweep_mt, returning elapsed TSC cycles.
// cpus[] (length num_threads) names the core for each thread; pass NULL to
// skip pinning.
uint64_t time_pull_sweep_mt(const csc_graph_t *g, const double *vprop,
                            double *out, int num_threads, const int *cpus) {
#ifdef _OPENMP
    omp_set_dynamic(0); // fixed team size -> the pinned thread pool is reused
#endif
    pin_team(num_threads, cpus);

    unsigned aux;
    uint64_t t0 = __rdtscp(&aux);
    pull_sweep_mt(g, vprop, out, num_threads);
    uint64_t t1 = __rdtscp(&aux);
    return t1 - t0;
}

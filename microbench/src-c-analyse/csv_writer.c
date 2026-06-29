#include "csr_sweep.h"

// CSV column order is the contract csr_analyze.py parses; keep header and row
// in lockstep. One header line per file, one row per timed run.

const char *dist_name(dist_t dist) {
    switch (dist) {
        case DIST_KRON:  return "kron";
        case DIST_URAND: return "urand";
    }
    return "urand";
}

void csv_write_header(FILE *out) {
    fprintf(out, "V,avg_degree,dist,kernel,threads,actual_E,run_idx,seed,"
                 "cycles,ns,ns_per_access\n");
}

// ns_per_access is ns / actual_E: every edge does exactly one random access
// (vprop[dst]++ for push, vprop[src] read for pull), so this is the headline
// per-random-access cost, not merely "per edge processed". `kernel` is
// "push"/"pull" and `threads` the team size (1 for push).
void csv_write_row(FILE *out, int64_t V, long long avg_degree, dist_t dist,
                   const char *kernel, int threads, uint64_t actual_E,
                   long long run_idx, uint64_t seed, uint64_t cycles,
                   double ns, double ns_per_access) {
    fprintf(out, "%lld,%lld,%s,%s,%d,%llu,%lld,%llu,%llu,%.3f,%.6f\n",
            (long long)V, avg_degree, dist_name(dist), kernel, threads,
            (unsigned long long)actual_E, run_idx, (unsigned long long)seed,
            (unsigned long long)cycles, ns, ns_per_access);
}

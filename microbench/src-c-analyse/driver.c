// Sweep driver: ties the modules together and runs one kernel across a grid.
//
// Picks ONE kernel per invocation (-kernel push|pull) and sweeps it over the
// avg_degree grid (repeatable -k), at fixed V. Push (scatter, single-thread)
// builds CSR and times vprop[col_idx]++; pull (gather, multi-thread) builds
// CSC and times out[v] = sum vprop[row_idx], swept over a thread-count axis
// (repeatable -t). Emits one unified CSV (csv_writer.c) to stdout or -o.
//
// Reuses: cli.c (parse_args), graph_generator.c (build_csr/build_csc),
// csr_sweep_utils.c (prefault, calibrate_tsc_ghz, memory_guard_ok),
// csv_writer.c, and the two kernel modules. See cli.c usage() for flags.
//
// Smoke test:
//   ./driver -V 100000 -k 8 -k 16 -r 3 -s 42 -kernel push -c 0
//   ./driver -V 100000 -k 16 -kernel pull -t 1 -t 2 -t 4 -c 0 -c 2 -c 4

#include "cli.h"
#include "csr_sweep.h"

// Run R timed push sweeps over the CSR graph and write one CSV row each.
static void run_push(FILE *out, const csr_graph_t *g, long long avg_degree,
                     dist_t dist, long long R, uint64_t seed, int cpu,
                     double ghz) {
    int64_t V = g->V;
    uint32_t *vprop = calloc((size_t)V, sizeof(uint32_t));
    size_t vprop_bytes = (size_t)V * sizeof(uint32_t);
    madvise(vprop, vprop_bytes, MADV_NOHUGEPAGE);

    for (long long run = 0; run < R; run++) {
        memset(vprop, 0, vprop_bytes);
        prefault(g->row_ptr, (size_t)(V + 1) * sizeof(uint32_t));
        prefault(g->col_idx, (size_t)g->actual_E * sizeof(uint32_t));
        prefault(vprop, vprop_bytes);

        uint64_t cycles = time_push_sweep_st(g, vprop, cpu);
        double ns = (double)cycles / ghz;
        double ns_per = g->actual_E ? ns / (double)g->actual_E : 0.0;
        csv_write_row(out, V, avg_degree, dist, "push", 1, g->actual_E, run,
                      seed, cycles, ns, ns_per);
    }

    uint64_t checksum = 0;
    for (int64_t i = 0; i < V; i++) checksum += vprop[i];
    fprintf(stderr, "# push checksum=%llu actual_E=%llu avg_degree=%lld\n",
            (unsigned long long)checksum, (unsigned long long)g->actual_E,
            avg_degree);
    free(vprop);
}

// Run R timed pull sweeps over the CSC graph for each thread count, writing
// one CSV row per (threads, run). vprop is the read array (double, init to 1.0
// so the gather is non-trivial and out[v] = in-degree(v)); out is the
// per-vertex private write target. Both are double so the gathered element is
// 8 bytes, matching real double-precision pull workloads (pagerank_pull).
static void run_pull(FILE *out, const csc_graph_t *g, long long avg_degree,
                     dist_t dist, long long R, uint64_t seed,
                     const long long *threads, size_t nthr,
                     const int *cpus, int ncpu, double ghz) {
    int64_t V = g->V;
    double *vprop = malloc((size_t)V * sizeof(double));
    double *vout = malloc((size_t)V * sizeof(double));
    size_t vprop_bytes = (size_t)V * sizeof(double);
    size_t vout_bytes = (size_t)V * sizeof(double);
    for (int64_t i = 0; i < V; i++) vprop[i] = 1.0;
    madvise(vprop, vprop_bytes, MADV_NOHUGEPAGE);

    for (size_t ti = 0; ti < nthr; ti++) {
        int t = (int)threads[ti];
        const int *pin = (cpus && ncpu >= t) ? cpus : NULL;
        if (cpus && ncpu < t) {
            fprintf(stderr, "warning: %d threads but only %d -c cores given; "
                            "running unpinned\n", t, ncpu);
        }
        for (long long run = 0; run < R; run++) {
            prefault(g->col_ptr, (size_t)(V + 1) * sizeof(uint32_t));
            prefault(g->row_idx, (size_t)g->actual_E * sizeof(uint32_t));
            prefault(vprop, vprop_bytes);
            prefault(vout, vout_bytes);

            uint64_t cycles = time_pull_sweep_mt(g, vprop, vout, t, pin);
            double ns = (double)cycles / ghz;
            double ns_per = g->actual_E ? ns / (double)g->actual_E : 0.0;
            csv_write_row(out, V, avg_degree, dist, "pull", t, g->actual_E,
                          run, seed, cycles, ns, ns_per);
        }
    }

    double checksum = 0.0;
    for (int64_t i = 0; i < V; i++) checksum += vout[i];
    fprintf(stderr, "# pull checksum=%.0f actual_E=%llu avg_degree=%lld\n",
            checksum, (unsigned long long)g->actual_E, avg_degree);
    free(vprop);
    free(vout);
}

int main(int argc, char **argv) {
    config_t cfg;
    if (parse_args(argc, argv, &cfg) != 0) return 1;

    FILE *out = stdout;
    if (cfg.out_path) {
        out = fopen(cfg.out_path, "w");
        if (!out) {
            fprintf(stderr, "failed to open -o %s\n", cfg.out_path);
            config_free(&cfg);
            return 1;
        }
    }

    uint64_t rng_state = cfg.seed ? cfg.seed : 1;
    double ghz = calibrate_tsc_ghz();
    csv_write_header(out);

    for (size_t ki = 0; ki < cfg.ndeg; ki++) {
        long long avg_degree = cfg.degrees[ki];
        uint64_t e_nominal = (uint64_t)cfg.V * (uint64_t)avg_degree;
        if (!memory_guard_ok(cfg.V, e_nominal, cfg.pull)) continue;

        fprintf(stderr, "V=%lld avg_degree=%lld kernel=%s dist=%s (nominal E=%llu)\n",
                cfg.V, avg_degree, cfg.kernel, cfg.dist_str,
                (unsigned long long)e_nominal);

        if (cfg.pull) {
            csc_graph_t g = build_csc(cfg.V, (uint64_t)avg_degree, false, cfg.dist, &rng_state);
            run_pull(out, &g, avg_degree, cfg.dist, cfg.runs, cfg.seed,
                     cfg.threads, cfg.nthr, cfg.cpus, cfg.ncpu, ghz);
            free_csc_graph(&g);
        } else {
            csr_graph_t g = build_csr(cfg.V, (uint64_t)avg_degree, false, cfg.dist, &rng_state);
            run_push(out, &g, avg_degree, cfg.dist, cfg.runs, cfg.seed,
                     cfg.push_cpu, ghz);
            free_csr_graph(&g);
        }
    }

    config_free(&cfg);
    if (out != stdout) fclose(out);
    return 0;
}

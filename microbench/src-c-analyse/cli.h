#ifndef CLI_H
#define CLI_H

#include "csr_sweep.h"

// Parsed driver configuration. The repeatable -k/-t/-c flags become owned
// arrays (freed by config_free). Strings point into argv (not owned).
typedef struct {
    long long V;
    long long runs;
    uint64_t seed;
    const char *out_path; // NULL -> stdout
    const char *dist_str; // for logging
    dist_t dist;
    const char *kernel;   // "push" / "pull" (for logging)
    int pull;             // 0 push, 1 pull

    long long *degrees;   // -k grid (defaulted if none given)
    size_t ndeg;
    long long *threads;   // -t axis, pull only (defaults to {1})
    size_t nthr;
    int *cpus;            // -c pin list
    int ncpu;
    int push_cpu;         // cpus[0] or -1 if none
} config_t;

// Parse argv into *cfg, applying defaults and validation. Returns 0 on
// success (cfg owns its arrays -> free with config_free), or 1 on error/bad
// usage (message already printed; nothing to free).
int parse_args(int argc, char **argv, config_t *cfg);
void config_free(config_t *cfg);

#endif

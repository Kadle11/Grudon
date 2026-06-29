#include "cli.h"

// Minimal dynamic long long list for the repeatable -k / -t / -c flags.
typedef struct {
    long long *vals;
    size_t len, cap;
} i64list;

static void list_push(i64list *l, long long v) {
    if (l->len == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 8;
        l->vals = realloc(l->vals, l->cap * sizeof(long long));
    }
    l->vals[l->len++] = v;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [-V <vertices>] [-k <avg_degree>]... [-kernel push|pull]\n"
            "          [-t <threads>]... [-dist urand] [-r <runs>] [-s <seed>]\n"
            "          [-c <cpu>]... [-o <out.csv>]\n"
            "\n"
            "  -V <vertices>      Vertex count, fixed for the sweep. Default 61000000.\n"
            "  -k <avg_degree>    Repeatable; nominal E = V*avg_degree. Default grid:\n"
            "                     1,2,4,8,16,24,32.\n"
            "  -kernel push|pull  Which kernel to sweep. Default push.\n"
            "  -t <threads>       Repeatable; pull thread-count axis. Default {1}.\n"
            "                     Ignored for push (always single-threaded).\n"
            "  -dist urand        Edge distribution. Only urand today.\n"
            "  -r <runs>          Timed runs per point. Default 5.\n"
            "  -s <seed>          splitmix64 seed. Default 42.\n"
            "  -c <cpu>           Repeatable; cores to pin to. Push uses the first;\n"
            "                     pull pins thread i to the i-th -c (give >= threads\n"
            "                     of them, ideally distinct physical cores). No -c ->\n"
            "                     no pinning.\n"
            "  -o <out.csv>       Output file (truncates). Default stdout.\n",
            prog);
}

int parse_args(int argc, char **argv, config_t *cfg) {
    *cfg = (config_t){0};
    cfg->V = 61000000;
    cfg->runs = 5;
    cfg->seed = 42;
    cfg->dist_str = "urand";
    cfg->kernel = "push";

    i64list klist = {0}, tlist = {0}, clist = {0};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-V") == 0 && i + 1 < argc) {
            cfg->V = atoll(argv[++i]);
        } else if (strcmp(argv[i], "-k") == 0 && i + 1 < argc) {
            list_push(&klist, atoll(argv[++i]));
        } else if (strcmp(argv[i], "-kernel") == 0 && i + 1 < argc) {
            cfg->kernel = argv[++i];
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            list_push(&tlist, atoll(argv[++i]));
        } else if (strcmp(argv[i], "-dist") == 0 && i + 1 < argc) {
            cfg->dist_str = argv[++i];
        } else if (strcmp(argv[i], "-r") == 0 && i + 1 < argc) {
            cfg->runs = atoll(argv[++i]);
        } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
            cfg->seed = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            list_push(&clist, atoll(argv[++i]));
        } else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            cfg->out_path = argv[++i];
        } else {
            goto fail;
        }
    }

    if (cfg->V <= 0 || cfg->runs <= 0) goto fail;

    if (strcmp(cfg->dist_str, "urand") != 0) {
        fprintf(stderr, "-dist '%s' not supported yet: only \"urand\"\n", cfg->dist_str);
        goto fail_quiet;
    }
    cfg->dist = DIST_URAND;

    if (strcmp(cfg->kernel, "push") == 0) {
        cfg->pull = 0;
    } else if (strcmp(cfg->kernel, "pull") == 0) {
        cfg->pull = 1;
    } else {
        fprintf(stderr, "-kernel '%s' invalid: use \"push\" or \"pull\"\n", cfg->kernel);
        goto fail_quiet;
    }

    if (klist.len == 0) {
        long long defaults[] = {1, 2, 4, 8, 16, 24, 32};
        for (size_t i = 0; i < sizeof(defaults) / sizeof(defaults[0]); i++)
            list_push(&klist, defaults[i]);
    }
    for (size_t i = 0; i < klist.len; i++) {
        if (klist.vals[i] <= 0) {
            fprintf(stderr, "-k avg_degree must be > 0\n");
            goto fail_quiet;
        }
    }

    if (cfg->pull && tlist.len == 0) list_push(&tlist, 1);
    for (size_t i = 0; i < tlist.len; i++) {
        if (tlist.vals[i] <= 0) {
            fprintf(stderr, "-t threads must be > 0\n");
            goto fail_quiet;
        }
    }

    cfg->degrees = klist.vals;
    cfg->ndeg = klist.len;
    cfg->threads = tlist.vals;
    cfg->nthr = tlist.len;

    cfg->ncpu = (int)clist.len;
    if (cfg->ncpu > 0) {
        cfg->cpus = malloc((size_t)cfg->ncpu * sizeof(int));
        for (int i = 0; i < cfg->ncpu; i++) cfg->cpus[i] = (int)clist.vals[i];
    }
    cfg->push_cpu = cfg->ncpu > 0 ? cfg->cpus[0] : -1;
    free(clist.vals);

    return 0;

fail:
    usage(argv[0]);
fail_quiet:
    free(klist.vals);
    free(tlist.vals);
    free(clist.vals);
    return 1;
}

void config_free(config_t *cfg) {
    free(cfg->degrees);
    free(cfg->threads);
    free(cfg->cpus);
}

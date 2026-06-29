#include "csr_sweep.h"

// SplitMix64 PRNG, used for generating random graphs. The state is a
// single 64-bit integer, and the output is a 64-bit random number. This
// is a simple, fast, and high-quality PRNG suitable for generating random
// graphs. The state should be initialized to a non-zero value before the
// first call to splitmix64. The state is updated in place, so the caller
// should maintain the state across calls to generate a sequence of random
// numbers.
static uint64_t splitmix64(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}


// Generates up to e_nominal uniform-random edges (src,dst ~ U[0,V)),
// packing each surviving (non-self-loop) edge into a single uint64_t key
// (src in the high 32 bits, dst in the low 32 bits). This packed form
// doubles as both the eventual col_idx payload (low 32 bits) and a sort
// key that orders by src first, dst second -- exactly the order CSR needs,
// and one that makes duplicate-edge detection a simple adjacent-pair scan
// after sorting (see build_csr_from_edges below). Returns the number of
// edges written to `edges` (<= e_nominal; self-loops are dropped here).
static uint64_t generate_edges_urand(uint64_t *edges, uint64_t e_nominal, int64_t V, uint64_t *rng_seed) {
    uint64_t n = 0;
    for (uint64_t e = 0; e < e_nominal; e++) {
        uint32_t src = (uint32_t)(splitmix64(rng_seed) % (uint64_t)V);
        uint32_t dst = (uint32_t)(splitmix64(rng_seed) % (uint64_t)V);
        if (src == dst) continue;
        edges[n++] = ((uint64_t)src << 32) | (uint64_t)dst;
    }
    return n;
}


// LSB radix sort (4 passes of 16 bits) over packed (key<<32|payload) edges,
// ordering by key first then payload -- exactly the grouping a CSR/CSC build
// needs. Uses `scratch` (same size as `edges`) as ping-pong space; after the
// 4 even passes the sorted data is back in `edges`, which is the pointer
// returned.
static uint64_t *radix_sort_edges(uint64_t *edges, uint64_t *scratch, uint64_t n) {
    uint64_t *count = malloc(65536 * sizeof(uint64_t));
    uint64_t *src = edges, *dst = scratch;
    for (int shift = 0; shift < 64; shift += 16) {
        memset(count, 0, 65536 * sizeof(uint64_t));
        for (uint64_t i = 0; i < n; i++) count[(src[i] >> shift) & 0xFFFF]++;
        uint64_t sum = 0;
        for (int b = 0; b < 65536; b++) {
            uint64_t c = count[b];
            count[b] = sum;
            sum += c;
        }
        for (uint64_t i = 0; i < n; i++) {
            uint16_t bucket = (uint16_t)((src[i] >> shift) & 0xFFFF);
            dst[count[bucket]++] = src[i];
        }
        uint64_t *tmp = src;
        src = dst;
        dst = tmp;
    }
    free(count);
    return src;
}


// Sorts `edges` (n entries) and squishes adjacent duplicates, returning the
// deduped count. `scratch` must be the same size as `edges`. The deduped
// result always ends up in `edges`.
static uint64_t sort_and_dedup_edges(uint64_t *edges, uint64_t *scratch, uint64_t n) {
    uint64_t *sorted = radix_sort_edges(edges, scratch, n);
    uint64_t m = 0;
    for (uint64_t i = 0; i < n; i++) {
        if (i == 0 || sorted[i] != sorted[i - 1]) edges[m++] = sorted[i];
    }
    return m;
}


// For each of the first n edges (src<<32|dst), append its reverse
// (dst<<32|src) so an undirected edge {u,v} is stored in both directions.
// The caller's `edges` buffer MUST have room for 2*n entries. Returns 2*n.
static uint64_t append_reverse_edges(uint64_t *edges, uint64_t n) {
    for (uint64_t i = 0; i < n; i++) {
        uint32_t src = (uint32_t)(edges[i] >> 32);
        uint32_t dst = (uint32_t)edges[i];
        edges[n + i] = ((uint64_t)dst << 32) | (uint64_t)src;
    }
    return 2 * n;
}


// Sort/dedup `edges` (m packed key<<32|payload entries) and compress them
// into a pointer/index pair: *ptr_out (V+1 entries) keyed by the high-32 key
// (0..V), *idx_out (E entries) the low-32 payload of each edge in sorted
// order. Allocates both arrays (caller frees) and returns the deduped edge
// count E. A temporary same-size scratch buffer is allocated for the sort.
static uint64_t build_compressed(uint64_t *edges, uint64_t m, int64_t V,
                                 uint32_t **ptr_out, uint32_t **idx_out) {
    uint64_t *scratch = malloc(m * sizeof(uint64_t));
    uint64_t E = sort_and_dedup_edges(edges, scratch, m);
    free(scratch);

    uint32_t *idx = malloc((size_t)E * sizeof(uint32_t));
    for (uint64_t k = 0; k < E; k++) idx[k] = (uint32_t)edges[k];

    uint32_t *ptr = malloc((size_t)(V + 1) * sizeof(uint32_t));
    uint64_t pos = 0;
    for (int64_t key = 0; key < V; key++) {
        ptr[key] = (uint32_t)pos;
        while (pos < E && (uint32_t)(edges[pos] >> 32) == (uint32_t)key) pos++;
    }
    ptr[V] = (uint32_t)pos;

    *ptr_out = ptr;
    *idx_out = idx;
    return E;
}


// Build a CSR graph from n packed (src<<32|dst) edges. If `undirected`, the
// reverse of every edge is appended first (so `edges` must hold 2*n), making
// the stored relation symmetric. Edges are then sorted, deduped, and grouped
// by source into row_ptr/col_idx (col_idx[] = destinations). outdeg follows
// directly from row_ptr; indeg is counted from the destinations.
static csr_graph_t build_csr_from_edges(uint64_t *edges, uint64_t n, int64_t V,
                                        bool undirected, dist_t dist) {
    uint64_t m = undirected ? append_reverse_edges(edges, n) : n;

    csr_graph_t g = {0};
    g.distribution = dist;
    g.V = V;
    g.actual_E = build_compressed(edges, m, V, &g.row_ptr, &g.col_idx);

    g.outdeg = malloc((size_t)V * sizeof(uint32_t));
    for (int64_t v = 0; v < V; v++) g.outdeg[v] = g.row_ptr[v + 1] - g.row_ptr[v];
    g.indeg = calloc((size_t)V, sizeof(uint32_t));
    for (uint64_t k = 0; k < g.actual_E; k++) g.indeg[g.col_idx[k]]++;

    return g;
}


// Build a CSC graph (transpose of CSR) from n packed (src<<32|dst) edges. We
// reuse the same compression by swapping each edge to (dst<<32|src), so the
// destination becomes the grouping key and the low-32 payload (-> row_idx)
// becomes the source (in-neighbour). If `undirected`, reverses are appended
// first (so `edges` must hold 2*n); the symmetric set makes CSC and CSR
// identical, as expected. indeg follows directly from col_ptr; outdeg is
// counted from the sources.
static csc_graph_t build_csc_from_edges(uint64_t *edges, uint64_t n, int64_t V,
                                        bool undirected, dist_t dist) {
    uint64_t m = undirected ? append_reverse_edges(edges, n) : n;
    for (uint64_t i = 0; i < m; i++) {
        uint32_t src = (uint32_t)(edges[i] >> 32);
        uint32_t dst = (uint32_t)edges[i];
        edges[i] = ((uint64_t)dst << 32) | (uint64_t)src;
    }

    csc_graph_t g = {0};
    g.distribution = dist;
    g.V = V;
    g.actual_E = build_compressed(edges, m, V, &g.col_ptr, &g.row_idx);

    g.indeg = malloc((size_t)V * sizeof(uint32_t));
    for (int64_t v = 0; v < V; v++) g.indeg[v] = g.col_ptr[v + 1] - g.col_ptr[v];
    g.outdeg = calloc((size_t)V, sizeof(uint32_t));
    for (uint64_t k = 0; k < g.actual_E; k++) g.outdeg[g.row_idx[k]]++;

    return g;
}


// Generate V*avg_degree nominal `dist` edges and build a CSR/CSC graph from
// them. The edge buffer (sized 2x for the undirected reverse-append) and the
// generator are owned here so callers never see them -- pass V/avg_degree/dist
// and get a finished graph. rng_state is the splitmix64 state, advanced in
// place so successive calls produce distinct (but seed-reproducible) graphs.
// Only DIST_URAND is generated today; kron slots into generate_edges below.
static uint64_t generate_edges(uint64_t *edges, uint64_t e_nominal, int64_t V,
                               dist_t dist, uint64_t *rng_state) {
    (void)dist; // only urand for now
    return generate_edges_urand(edges, e_nominal, V, rng_state);
}

csr_graph_t build_csr(int64_t V, uint64_t avg_degree, bool undirected,
                      dist_t dist, uint64_t *rng_state) {
    uint64_t e_nominal = (uint64_t)V * avg_degree;
    uint64_t cap = undirected ? 2 * e_nominal : e_nominal;
    uint64_t *edges = malloc(cap * sizeof(uint64_t));
    uint64_t n = generate_edges(edges, e_nominal, V, dist, rng_state);
    csr_graph_t g = build_csr_from_edges(edges, n, V, undirected, dist);
    free(edges);
    return g;
}

csc_graph_t build_csc(int64_t V, uint64_t avg_degree, bool undirected,
                      dist_t dist, uint64_t *rng_state) {
    uint64_t e_nominal = (uint64_t)V * avg_degree;
    uint64_t cap = undirected ? 2 * e_nominal : e_nominal;
    uint64_t *edges = malloc(cap * sizeof(uint64_t));
    uint64_t n = generate_edges(edges, e_nominal, V, dist, rng_state);
    csc_graph_t g = build_csc_from_edges(edges, n, V, undirected, dist);
    free(edges);
    return g;
}

void free_csr_graph(csr_graph_t *g) {
    free(g->row_ptr);
    free(g->col_idx);
    free(g->outdeg);
    free(g->indeg);
}

void free_csc_graph(csc_graph_t *g) {
    free(g->col_ptr);
    free(g->row_idx);
    free(g->outdeg);
    free(g->indeg);
}






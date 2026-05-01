#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <stddef.h>

#define DAMPING_FACTOR 0.85f
#define THRESHOLD 1e-5f

typedef uint32_t vid_t;

// Vertex Properties
typedef struct {
    float pr;          
    float delta;       
    float update_sum;  
} VProp;

// Standard CSR Graph loaded into CXL memory
typedef struct {
    vid_t num_vertices;
    size_t num_edges;
    
    // Regular CSR Format
    size_t* row_ptr; 
    vid_t* col_idx;  
    int* out_degree; 
} CXL_Graph;

typedef struct {
    uint32_t cid;
    int opcode;
    vid_t num_vertices;
    uint32_t* frontier_ndp;
    VProp* vprops_mirror;
    CXL_Graph* graph;
} command_entry_t;

#define OPCODE_GEN_UPDATES 1

// Bitmask Helpers
static inline void set_bit(uint32_t* mask, vid_t u) {
    __atomic_fetch_or(&mask[u / 32], (1U << (u % 32)), __ATOMIC_RELAXED);
}
static inline int get_bit(uint32_t* mask, vid_t u) {
    return (mask[u / 32] & (1U << (u % 32))) != 0;
}

#endif // TYPES_H


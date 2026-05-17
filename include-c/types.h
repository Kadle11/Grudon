#ifndef TYPES_H
#define TYPES_H

#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>

#define DAMPING_FACTOR 0.85f
#define THRESHOLD      1e-5f

typedef uint32_t vid_t;

// Vertex Properties
typedef struct
{
  float pr;     // NOTE: the code uses this as the 'push' contribution value!
  float delta;  // The accumulated residual
  float score;  // The actual PageRank score
  float update_sum;
} VProp;

// RankPair for sorting final output
typedef struct
{
  vid_t vertex_id;
  float score;
} RankPair;

// Standard CSR Graph loaded into CXL memory
typedef struct
{
  vid_t num_vertices;
  size_t num_edges;

  // Regular CSR Format
  size_t* row_ptr;
  vid_t* col_idx;
  int* out_degree;
  // Symmetric CSR (undirected view). When present use these for undirected algorithms.
  size_t* row_ptr_sym;
  vid_t* col_idx_sym;
  int is_symmetric;
} CXL_Graph;

// Command structure for NDP offload
typedef struct
{
  uint32_t cid;
  int opcode;
  vid_t num_vertices;
  uint32_t* frontier_ndp;
  VProp* vprops_mirror;
  CXL_Graph* graph;
} command_entry_t;

#define OPCODE_GEN_UPDATES_PR 1
#define OPCODE_GEN_UPDATES_CC 2
#define OPCODE_GEN_UPDATES_SSSP 3

// Bitmask Helpers
static inline void set_bit(uint32_t* mask, vid_t u)
{
  __atomic_fetch_or(&mask[u / 32], (1U << (u % 32)), __ATOMIC_RELAXED);
}
static inline int get_bit(uint32_t* mask, vid_t u)
{
  return (mask[u / 32] & (1U << (u % 32))) != 0;
}

#endif  // TYPES_H

#ifndef GRAPH_IO_H
#define GRAPH_IO_H

#include "../include-c/types.h"

CXL_Graph* read_graph_serial(const char* filename, int build_symmetric);
void destroy_graph(CXL_Graph* graph);

const size_t* graph_row_ptr(const CXL_Graph* graph, int use_symmetric);
const vid_t* graph_col_idx(const CXL_Graph* graph, int use_symmetric);

#endif // GRAPH_IO_H

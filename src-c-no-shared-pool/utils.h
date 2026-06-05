#ifndef UTILS_H
#define UTILS_H

#include "../include-c/types.h"

void write_measurement_flag(int flag);
void print_pagerank_summary(const CXL_Graph* graph, const VProp* props);
void print_connected_components_summary(const CXL_Graph* graph, const VProp* props);
void print_sssp_summary(const CXL_Graph* graph, const VProp* props);
void wait_for_profiler_ack(unsigned timeout_ms);
int profiling_enabled(void);

#endif // UTILS_H

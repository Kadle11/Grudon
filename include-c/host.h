#ifndef HOST_H
#define HOST_H

#include "device.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <sys/time.h>

#define MAX_ITERATIONS 1000

CXL_Graph* read_graph(const char* filename);

#endif // HOST_H

#ifndef QUICKSORT_H
#define QUICKSORT_H

#include <stddef.h>
#include "types.h"

// Quicksort function to sort vertices by PageRank score for final output
void quicksort(RankPair* arr, size_t count);

#endif  // QUICKSORT_H

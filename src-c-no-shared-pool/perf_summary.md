# Perf Summary

Averaged inclusive stats from bench_*.data files, grouped by algorithm then dataset.

## cc / europe-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 6,724,667,720,154 | 549,379,303,857 | 0.08 |
| 2 | update_frontier_cc | 6,145,471,189,441 | 378,491,080,236 | 0.06 |
| 3 | generate_updates_algo | 504,278,080,906 | 308,836,467,460 | 0.61 |
| 4 | run_algo | 1,527,132,344 | 929,466,004 | 0.61 |

## cc / italy-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 1,115,157,814,381 | 76,701,944,742 | 0.07 |
| 2 | update_frontier_cc | 1,011,948,306,009 | 47,519,608,365 | 0.05 |
| 3 | generate_updates_algo | 63,510,743,571 | 38,829,911,564 | 0.61 |
| 4 | run_algo | 309,094,377 | 165,157,623 | 0.53 |

## cc / kron-g500-logn21

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 3,941,894,617 | 202,827,710 | 0.05 |
| 2 | update_frontier_cc | 3,689,113,684 | 72,184,508 | 0.02 |
| 3 | generate_updates_algo | 2,286,940,289 | 1,103,259,672 | 0.48 |
| 4 | run_algo | 56,750,798 | 31,043,439 | 0.55 |

## cc / soc-LiveJournal1

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 15,509,834,371 | 899,877,507 | 0.06 |
| 2 | update_frontier_cc | 14,980,071,856 | 419,108,222 | 0.03 |
| 3 | generate_updates_algo | 3,047,211,976 | 2,355,176,432 | 0.77 |
| 4 | run_algo | 43,481,094 | 63,887,591 | 1.47 |

## pr / europe-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 882,738,261,913 | 59,098,423,433 | 0.07 |
| 2 | update_frontier_pr | 811,081,729,050 | 28,369,572,791 | 0.03 |
| 3 | generate_updates_algo | 71,501,654,322 | 61,345,467,317 | 0.86 |
| 4 | run_algo | 1,061,907,851 | 766,670,692 | 0.72 |

## pr / italy-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | update_frontier_pr | 162,489,974,458 | 3,678,364,645 | 0.02 |
| 2 | apply_updates_algo | 160,166,658,228 | 9,379,060,092 | 0.06 |
| 3 | generate_updates_algo | 10,812,945,839 | 9,754,056,279 | 0.90 |
| 4 | run_algo | 146,975,872 | 102,804,357 | 0.70 |

## pr / kron-g500-logn21

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | generate_updates_algo | 40,226,945,346 | 8,958,433,952 | 0.22 |
| 2 | update_frontier_pr | 5,339,855,462 | 340,174,793 | 0.06 |
| 3 | apply_updates_algo | 5,253,956,682 | 646,104,933 | 0.12 |
| 4 | run_algo | 31,724,451 | 25,896,350 | 0.82 |

## pr / soc-LiveJournal1

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | generate_updates_algo | 194,114,152,617 | 60,112,182,888 | 0.31 |
| 2 | update_frontier_pr | 153,709,449,165 | 3,431,699,180 | 0.02 |
| 3 | apply_updates_algo | 147,424,079,529 | 10,083,002,531 | 0.07 |
| 4 | run_algo | 131,572,859 | 80,348,484 | 0.61 |

## sssp / europe-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 709,356,320,680 | 463,239,826,281 | 0.65 |
| 2 | update_frontier_sssp | 572,788,259,345 | 406,206,175,592 | 0.71 |
| 3 | generate_updates_algo | 168,884,442,523 | 348,466,862,235 | 2.06 |
| 4 | run_algo | 589,835,114 | 394,522,790 | 0.67 |

## sssp / italy-osm

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | update_frontier_sssp | 81,752,885,729 | 52,566,539,337 | 0.64 |
| 2 | apply_updates_algo | 73,940,892,864 | 59,285,547,735 | 0.80 |
| 3 | generate_updates_algo | 22,446,553,533 | 46,805,868,456 | 2.09 |
| 4 | run_algo | 164,738,807 | 83,045,023 | 0.50 |

## sssp / kron-g500-logn21

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 1,634,658,094 | 128,864,709 | 0.08 |
| 2 | update_frontier_sssp | 1,498,356,509 | 80,604,482 | 0.05 |
| 3 | generate_updates_algo | 687,655,323 | 373,571,579 | 0.54 |
| 4 | run_algo | 24,419,226 | 17,917,675 | 0.73 |

## sssp / soc-LiveJournal1

| Rank | Function | Avg Cycles | Avg Instructions | IPC |
| --- | --- | ---: | ---: | ---: |
| 1 | apply_updates_algo | 3,873,833,375 | 691,298,439 | 0.18 |
| 2 | update_frontier_sssp | 3,718,867,948 | 546,062,665 | 0.15 |
| 3 | generate_updates_algo | 824,671,822 | 703,906,544 | 0.85 |
| 4 | run_algo | 34,658,869 | 43,543,078 | 1.26 |


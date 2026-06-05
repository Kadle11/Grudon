### PageRank Profiling Summary (Averaged over 3 runs)

| Graph | Function Name | Avg Cycles | Avg Instructions | IPC | % of Target (Cycles) |
|---|---|---|---|---|---|
| **Soc-Live Journal** <br>*(Overall: IPC 0.86)* | `generate_updates_pagerank` | 47,341,404,538 | 32,056,117,874 | 0.68 | 76.11% |
| | `apply_updates_pagerank.isra.0` | 4,928,936,031 | 7,053,149,388 | 1.43 | 7.92% |
| | `set_bit` | 4,400,595,895 | 3,059,542,083 | 0.70 | 7.07% |
| | `get_bit` | 4,036,577,362 | 8,921,846,585 | 2.21 | 6.49% |
| | `update_frontier_pagerank` | 1,246,776,714 | 2,087,380,321 | 1.67 | 2.00% |
| | | | | | |
| **italy-osm** <br>*(Overall: IPC 1.67)* | `apply_updates_pagerank.isra.0` | 3,752,653,537 | 5,614,748,381 | 1.50 | 25.89% |
| | `get_bit` | 3,426,105,786 | 8,542,139,489 | 2.49 | 23.64% |
| | `generate_updates_pagerank` | 3,080,934,705 | 5,422,421,995 | 1.76 | 21.26% |
| | `set_bit` | 3,000,204,538 | 2,103,777,707 | 0.70 | 20.70% |
| | `update_frontier_pagerank` | 958,541,513 | 2,076,884,897 | 2.17 | 6.61% |
| | | | | | |
| **kron_g500-logn21** <br>*(Overall: IPC 0.89)* | `generate_updates_pagerank` | 3,900,722,026 | 2,647,896,933 | 0.68 | 77.61% |
| | `get_bit` | 430,829,178 | 917,340,170 | 2.13 | 8.57% |
| | `apply_updates_pagerank.isra.0` | 424,747,798 | 460,452,857 | 1.08 | 8.45% |
| | `update_frontier_pagerank` | 132,260,404 | 315,412,658 | 2.38 | 2.63% |
| | `set_bit` | 94,549,249 | 65,758,030 | 0.70 | 1.88% |

*(Note: Data is drawn from the "Averaged Cycles" sections, as these natively track the actual hardware execution time).*
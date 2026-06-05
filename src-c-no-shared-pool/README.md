# CXL PageRank Emulation

This directory contains a single-process, serial implementation of PageRank, Connected Components, and SSSP using a simple vertex-programming loop.

## Project Structure

- `host.c` - Self-contained graph reader, three-step algorithm loop, and result reporting.
- `profile-implementation` - Helper script that watches `./measurement` and starts `perf record` when the program is ready.
- `measurement` - Local handshake file written by the binary to coordinate profiling.

## Prerequisites

- GCC or another standard C compiler.
- `make`

## How to Compile and Run

A `Makefile` is provided to compile the serial host runner.

**To compile the project:**
```bash
make
```

**To run the algorithms:**
```bash
./run_graph_algorithms <graph_file.mtx> <algorithm> [--symmetric|-s]
```

Supported algorithms are `pr` or `pagerank`, `cc` or `components`, and `sssp` or `shortest_path`.

**To collect a profiling trace:**
```bash
bash ./profile-implementation <algo> [graph_path] [--dry-run]
```

**To clean up build artifacts:**
```bash
make clean
```

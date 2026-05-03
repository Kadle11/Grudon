# CXL PageRank Emulation

This directory contains a C-based implementation of a PageRank emulation over CXL.

## Project Structure

- `host.c` - Main host execution, handling memory allocation, graph loading, and the PageRank logic loop.
- `device.c` - Device-side PageRank logic (NDP - Near Data Processing).
- `read_graph.c` - Utilities to read `.mtx` files into the core CXL Graph structures.
- `cxl_utils.c` - Mocking/utilities for CXL memory and command passing.
- `sorting.c` - Utility functions for sorting.
- `soc-LiveJournal1/` - Contains the dataset to test with.

## Prerequisites

- GCC or another standard C compiler.
- `make`

## How to Compile and Run

A `Makefile` is provided to compile and run the application. 

**To compile the project:**
```bash
make
```

**To compile AND run the project using the provided dataset (`soc-LiveJournal1.mtx`):**
```bash
make run
```

**To clean up build artifacts:**
```bash
make clean
```

# Grudon
Grudon is a distributed graph processing framework built on top of MPI and
inspired by the Galois framework. It aims to provide efficient and scalable
graph analytics on large-scale graphs on emerging Disaggregated architectures
equipped with computational memory.

## Prerequisites

- C++ compiler with C++17 support (gcc/clang)
- CMake 3.15 or newer
- make
- openmpi + mpirun `sudo apt-get install openmpi-bin openmpi-doc libopenmpi-dev`
- Boost libraries (v1.74 or newer)
- llvm

- Input graph in .mtx format (Matrix Market format) 

## Quick build

```bash
git clone https://github.com/Kadle11/Grudon.git
cd Grudon

git submodule update --init --recursive
cd extern/Galois
git checkout Grudon

# Execute in Grudon Repo ROOT 
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug # Release Build Coming Soon
make -j$(nproc)
```


## Running Grudon

First, we need to convert and partition the input graph to a binary format that
Grudon can read. We use Galois's tooling for this purpose so that we can match
the partitioning scheme used by Galois to make fair comparisons.

```bash
# First build Galois
cd extern/Galois
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# Run the conversion and partitioning script to generate the GRAPH_PARTS_FILE (Used below)
./scripts/convert_and_partition.sh <input_graph.mtx> <num_partitions> <output_prefix>

# Example Command for Quad Socket Intel Xeon with 20 cores/socket and 2 threads/core
mpirun -n 4 --npersocket 1 --map-by NUMA:PE=20 --use-hwthread-cpus --report-bindings ./bin/Debug/Groudon -g $GRAPH_PATH -c 1 -m 3 -t 20 -p $GRAPH_PARTS_FILE -a pr
```

## Emulating the DiNDP environment
Please look at the `emulation_harness` directory for scripts to emulate the DiNDP
environment on a multi-socket system.

---
For more information please refer to our paper:
```bib
@inproceedings{rao2025grudon,
  title={Grudon: A System for Deploying Graph Workloads on Disaggregated Architectures with Near-Data Processing},
  author={Rao, Vishal and Shashidhar, Nikhil Ram and Lee, Suyeon and Gavrilovska, Ada},
  booktitle={Proceedings of the 34th International Symposium on High-Performance Parallel and Distributed Computing},
  pages={1--14},
  year={2025}
}
```

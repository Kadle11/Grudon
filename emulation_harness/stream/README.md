## STREAM*

This benchmark is a slightly modified version of [https://github.com/host-architecture/understanding-the-host-network/tree/master/stream]() used to inject a configurable amount of memory bandwidth contention in our emulation. It supports different read/write ratios and memory access patterns.

### Requirements

The benchmark code currently uses AVX-512/AVX-2 to generate load/store instructions, and therefore requires a processor with AVX support. You can check whether your processor provides the necessary support using the following:
```
cat /proc/cpuinfo | grep avx
```

### Compiling

To compile, simply run
```
make
```

### Usage

To run the benchmark:
```
./stream <workload> <duration>
```

For example, to run 64-byte sequential read workload for 10 seconds:
```
./stream Read64 10
```

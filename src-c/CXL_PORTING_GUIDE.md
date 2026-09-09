# Porting a System to a CXL Memory Pool with Near-Data Compute

A working reference: the concepts, what Grudon actually did in `src-c/`, and the
checklist for doing it again on another codebase with ARM cores on the device.

---

## Part I — CXL concepts you actually need

### The three protocols
CXL runs three protocols over a PCIe PHY:

| Protocol | What it carries | Who initiates |
|---|---|---|
| `CXL.io` | PCIe-equivalent: discovery, config, DMA, interrupts | Host |
| `CXL.cache` | Device caching *host* memory, coherently | Device |
| `CXL.mem` | Host load/store into *device* memory (HDM) | Host |

Device types are just which protocols are implemented:
- **Type 1** — `.io` + `.cache`. Accelerator, no local memory (NIC, SmartNIC).
- **Type 2** — `.io` + `.cache` + `.mem`. Accelerator **with** memory. This is
  what "memory pool with ARM cores" is. Both sides can touch both memories.
- **Type 3** — `.io` + `.mem`. Dumb memory expander/pool. No device compute.

**If your target has ARM cores on the memory pool, you are on a Type 2 device
(or a Type 3 pool with a separate SoC attached to it).** The distinction matters
because Type 2 is where coherence gets hard.

### How the pool appears to software
Device memory is exposed as **HDM** (Host-managed Device Memory), mapped into
the host physical address space. Linux surfaces it as either:
- a **CPU-less NUMA node** (`numactl -H` shows a node with `0 MB` of CPU), or
- a **DAX device** (`/dev/dax0.0`) that you `mmap` explicitly.

Which one you get changes your allocation strategy completely — see Step 2.

### The numbers that drive every design decision
Order-of-magnitude, current-generation hardware. Measure your own (Part IV).

| Path | Idle latency | Bandwidth |
|---|---|---|
| Local DRAM | ~80–100 ns | ~25 GB/s per channel |
| Remote NUMA socket | ~140–200 ns | ~limited by UPI/IF |
| CXL pool (Type 3, direct) | ~250–400 ns | x8 Gen5 ≈ 32 GB/s, x16 ≈ 64 GB/s |
| CXL pool behind a switch | ~400–700 ns | same, plus switch queueing |

Two consequences:
1. **CXL is a latency device, not a bandwidth device.** Sequential streaming over
   CXL is nearly as fast as local DRAM. Pointer-chasing over CXL is 3–5x worse.
   Graph workloads are pointer-chasing. This is the whole reason NDP exists.
2. The link is **narrow relative to a memory controller**. Moving the working set
   back and forth per iteration will saturate it. Move *commands*, not data.

### Coherence — the part that bites
- **HDM-H** (host-only coherent): host caches HDM; device must not cache it, or
  must be told to flush. Software manages it.
- **HDM-D / HDM-DB**: hardware-coherent; device participates via `.cache` or a
  back-invalidate channel. Real, but slow, and not universally available.
- **Bias modes** (Type 2): *host bias* — device accesses to its own memory are
  routed through the host's home agent (correct, slow). *Device bias* — device
  accesses go straight to local memory (fast), host must not have dirty lines.
  Flipping bias is an explicit, expensive operation.
- **Pooled/shared across hosts** (MLD, multi-headed devices, fabric manager):
  **there is generally no hardware coherence between hosts.** Sharing is by
  software protocol: writer flushes, reader invalidates, and an out-of-band
  message orders the two.

> **The one rule.** Never assume the device sees your store just because the
> store retired. Every producer→consumer handoff needs an explicit
> flush/invalidate plus an ordering message. Grudon encodes this as
> `cxl_flush_range()` + a command/ack round-trip.

### Addressing
The host and the device map the pool at **different virtual addresses**. A
pointer stored in the pool by the host is meaningless to the device.
Everything crossing the boundary must be a **base-relative offset**.

### What NDP buys you
Offloading is profitable when, for a kernel:

```
  bytes_touched_by_kernel / link_BW  >>  device_compute_time - host_compute_time
```

i.e. the kernel touches far more data than it produces, and the device cores are
not *so* much weaker that they eat the savings. Irregular gather/scatter over a
large structure is the sweet spot. Compute-dense, cache-resident kernels are not.

---

## Part II — What Grudon actually did

### The starting point
- `src/`, `include/` — the original C++/MPI Grudon: `UpdateWorker`,
  `TraverseWorker`, `AggregateWorker` across MPI ranks, DiNDP *emulated* on a
  multi-socket box via `emulation_harness/`.
- `src-c-no-shared-pool/` — a **barebones C rewrite, single process, no CXL**.
  Same three algorithms (`pr`, `cc`, `sssp`), same `CXL_Graph` type.
- `src-c/` — the same barebones code, split across a shared pool and a device
  process.

**The single most important structural decision: `src-c-no-shared-pool/` exists.**
It is a functionally identical, in-tree, non-CXL baseline built from the same
`include-c/types.h`. Every CXL number is meaningful only against it. Do this
first on the next system.

### The layering
```
  host.c        pagerank.c / cc.c / sssp.c      <- host-side: init + apply
  read_graph.c  sorting.c                        <- host-side: ingest
      |
      v
  include-c/device.h   <-- "Machine API": the seam
      |
      +-- cxl_utils.c            (mock: bump allocator over an mmap'd pool)
      +-- shared_memory_utils.c  (mmap + ptr<->offset)
      +-- ipc.c                  (command/ack over a socketpair)
      |
      v
  device.c      gen_updates_{pagerank,cc,sssp}_push  <- device-side kernels
```

`include-c/device.h` declares exactly the surface a real machine must provide:

```c
void* cxl_malloc(size_t);  void* cxl_calloc(size_t, size_t);  void cxl_free(void*);
void  cxl_flush_range(void* ptr, size_t size);
void  cxl_send_cmd(const command_entry_t* cmd);
void  cxl_wait_done(uint32_t cid);
void  cxl_memcpy_to_device(void* dst, const void* src, size_t);
void  cxl_memcpy_to_host  (void* dst, const void* src, size_t);
```

**Porting to real hardware = reimplementing `cxl_utils.c` and `ipc.c`.**
Nothing in `host.c`, `pagerank.c`, `cc.c`, `sssp.c` or `device.c` changes.
That is the payoff of the seam. Build it before you need it.

### 1. The pool — `shared_memory_utils.c`, `cxl_utils.c`
```c
mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0)
```
`MAP_SHARED` is load-bearing: it survives `fork()` as one physical region.
`MAP_PRIVATE` would silently give the device a copy-on-write private copy — the
program would run and produce wrong answers.

On top of it, a **bump allocator**, 64-byte aligned:
```c
ptr = base + offset;  offset += (size + 63) & ~63;
```
`cxl_free()` is a **no-op while the pool is active**. Arena semantics: allocate
everything up front, tear down the whole region at the end. This is the right
model for a pool — a real pool allocator is a fabric-manager operation, not a
per-object one, and fragmenting a pooled region is far more expensive than
wasting some of it.

### 2. Data placement — the actual split
| In the pool (`cxl_malloc`) | On the host heap (`malloc`) |
|---|---|
| `CXL_Graph` struct | `vprop_masters` (host's authoritative copy) |
| `row_ptr`, `col_idx`, `out_degree` | `frontier_host` |
| `row_ptr_sym`, `col_idx_sym` | `srcs`, `dsts`, `next_pos`, `deg_sym` (ingest scratch) |
| `vprop_mirrors` (device's copy) | `ranks` (output sorting) |
| `frontier_ndp` (device's copy) | |

The rule applied: **the pool holds what the device reads irregularly and
repeatedly** (the CSR, which is read every iteration and never written). The host
heap holds transient scratch and the host's own linear-scan working set.

### 3. Master/mirror — software coherence made explicit
Two copies of the mutable state, by design:
- `vprop_masters` / `frontier_host` — host heap, host-owned.
- `vprop_mirrors` / `frontier_ndp` — pool, device-owned during a command.

Per iteration (`host.c`):
```
  cxl_send_cmd(&cmd);        // hand ownership to the device
  cxl_wait_done(cmd.cid);    // block until it hands it back
  cxl_memcpy_to_host(vprop_masters, vprop_mirrors, ...);
  apply_updates_*(...);      // host-only phase, on host memory
  cxl_memcpy_to_device(frontier_ndp,  frontier_host,  ...);
  cxl_memcpy_to_device(vprop_mirrors, vprop_masters, ...);
```
and `cxl_memcpy_to_{device,host}` wrap the copy in `cxl_flush_range()` on both
sides. There is exactly one writer at any instant, and the transition is a
message. That is the entire coherence protocol, and it is enough.

### 4. Pointers → offsets — `ipc.c`
`command_entry_t` holds real pointers on the host side. `send_command_to_device`
translates every one before it crosses:
```c
msg.payload.cmd.frontier_off = pool_ptr_to_offset(ipc_pool_base, cmd->frontier_ndp);
msg.payload.cmd.vprops_off   = pool_ptr_to_offset(ipc_pool_base, cmd->vprops_mirror);
msg.payload.cmd.graph_off    = pool_ptr_to_offset(ipc_pool_base, cmd->graph);
```
and `device_process_main` translates back with `pool_offset_to_ptr(pool_base, …)`.

> ⚠️ **Known limitation to fix when porting.** `CXL_Graph`'s *internal* fields
> (`row_ptr`, `col_idx`, `out_degree`, `row_ptr_sym`, `col_idx_sym`) are stored
> as raw pointers inside a pool-resident struct, and `device.c` dereferences them
> directly. This works here only because `fork()` gives the child the identical
> mapping address. On real hardware the device maps the pool elsewhere and these
> dereference into garbage. **Any pointer stored *inside* the pool must be an
> offset too**, resolved through the local base on each side.

### 5. Host and device as separate processes
`host.c` does `socketpair(AF_UNIX, SOCK_SEQPACKET)` + `fork()`. The child pins
itself to CPU 3 and enters `device_process_main`; the parent pins to CPU 2.

Why a process and not a thread — all three reasons matter:
1. **Separate address space** forces the offset discipline to be exercised.
2. **Separate scheduling domain** lets you pin device work to a distinct core set
   and emulate the device's core count and frequency independently.
3. **Separate PID** lets `perf` attribute host vs device cycles separately.

`SOCK_SEQPACKET` gives message boundaries — a fixed-size `ipc_msg_t` is either
fully delivered or not at all, so no framing code.

### 6. The command protocol — a doorbell in software
```c
typedef struct { uint32_t cid; int opcode; vid_t num_vertices;
                 uint32_t* frontier_ndp; VProp* vprops_mirror;
                 CXL_Graph* graph; } command_entry_t;
```
`cxl_send_cmd` is asynchronous; `cxl_wait_done(cid)` blocks on a `cid`-matched
`IPC_MSG_ACK`, ignoring unrelated messages. This mirrors a real submission
queue + doorbell + completion queue, so swapping in an MMIO doorbell later is a
change to `ipc.c` only. Note the loop **already tolerates out-of-order
completions** even though the current host issues one command at a time — that is
what makes pipelining a later, local change.

### 7. Which half runs where
From `profiling_summary.md` (soc-LiveJournal1, PageRank):

| Phase | % cycles | IPC | Runs on |
|---|---|---|---|
| `generate_updates_pagerank` (edge scatter) | **76.1%** | **0.68** | **Device** |
| `apply_updates_pagerank` (vertex linear scan) | 7.9% | 1.43 | Host |
| `set_bit` / `get_bit` (frontier) | 13.6% | 0.70 / 2.21 | split |

The offloaded phase is the one with **low IPC and a large irregular footprint** —
memory-stalled, so weaker device cores cost little, and it touches the whole CSR,
so keeping it near the data saves the most traffic. The host keeps the
cache-friendly, high-IPC linear phase. **Pick your offload boundary from a
profile, not from intuition.** Note also `italy-osm` inverts this (overall IPC
1.67, `generate_updates` only 21%) — a low-degree road network is a bad NDP
candidate. Offload profitability is per-input, not just per-kernel.

### 8. Device-side atomics
- `set_bit`: `__atomic_fetch_or(..., __ATOMIC_RELAXED)` on the frontier word.
- `atomic_min_float`: CAS loop on the `uint32_t` bit-pattern of the float
  (there is no native FP atomic min) — used by `cc` and `sssp`.
- PageRank's `delta` accumulate: `#pragma omp atomic`.

All of these are **device-local**: they are only ever executed by the device's
OpenMP threads, on pool memory, while the host is blocked in `cxl_wait_done`.
Atomics that would have to span the host and the device across the CXL link are
avoided entirely. Keep it that way (see Don'ts).

### 9. Affinity and measurement
`host.c` writes a handshake file `/tmp/measurement`:
```
  1                 <- flag: 1 = steady state reached, 0 = done
  <device_pid>,<host_pid>
```
`profile-implementation` polls it at 1 ms, then starts
`perf record -g -p <device_pid>,<host_pid> -e cycles,instructions`, pinned to its
own core, with a watcher on another core that `SIGINT`s perf when the flag flips
to 0. Three runs, `drop_caches` between each. Core budget is explicit:

| Role | Cores |
|---|---|
| Watcher | 0 |
| perf | 1 |
| Host | 2 |
| Device + OpenMP (`GOMP_CPU_AFFINITY`) | 3–7, 16–23 |

This excludes graph ingest and teardown from the trace — otherwise `.mtx` parsing
dominates. **Instrument the steady-state region, not the process.**

### 10. Emulating the environment before you have hardware
`emulation_harness/` throttles a normal multi-socket box into a plausible
disaggregated one:
- `cpu_freq_set.sh <mhz_per_socket>` — down-clock one socket → weak device cores.
- `memory_bandwidth_set.sh <mlc> <stream> <pct_per_socket>` — calibrates with
  Intel MLC, then parks STREAM instances on selected cores to burn a target
  fraction of bandwidth → narrow CXL link.
- `smt_toggle.sh {enable|disable} <numa_node>` — device core-count control.

Combined with `numactl --membind` to a remote socket, you get a decent CXL stand-in
(remote-NUMA latency is ~half of real CXL, so treat it as an optimistic bound).

---

## Part III — Porting the next system: steps

### Step 0 — Build the non-CXL baseline first
Strip to a single-process version with no CXL calls, same data structures, same
outputs. Keep it in-tree and keep it building. Everything is measured against it.

### Step 1 — Profile it and find the offload boundary
`perf record -g` the steady state. You are looking for a phase that is:
- a large share of cycles, **and** low IPC (memory-stalled), **and**
- touches a large read-mostly structure, **and**
- has a small input/output surface relative to what it reads.

If no phase fits, **the workload is not an NDP candidate** — say so and stop.

### Step 2 — Decide how the pool is surfaced, and allocate accordingly
| Exposure | Allocation |
|---|---|
| CPU-less NUMA node | `numa_alloc_onnode()` / `mbind()`; verify with `move_pages()` |
| DAX device | `open("/dev/dax0.0")` + `mmap`, or `memkind`/`libvmem` |
| Cross-host shared pool | fabric-manager-assigned region, `mmap` by both hosts |
Wrap it behind `cxl_malloc`/`cxl_free` regardless. Arena, not general-purpose.

### Step 3 — Classify every allocation
Go through them one by one and tag each: **pool**, **host-private**, or
**device-private**. Write the table down. Read-mostly + device-touched → pool.
Transient ingest scratch → host heap, always.

### Step 4 — Cut the Machine API seam
Reproduce `include-c/device.h`. Implement it twice: a `fork`+`mmap` mock (for
development on any laptop) and the real one. Keep both compiling.

### Step 5 — Make the boundary offset-clean
Every pointer that crosses **or is stored inside** the pool becomes a
`uint64_t` offset. Resolve through the local base. Grep for pool-resident structs
containing `*` and fix them (see the `CXL_Graph` limitation above).

### Step 6 — Define ownership and the handoff
One writer per region at a time. Handoff = flush + message + ack. Write the
ownership table into a comment at the top of the file that owns the loop.

### Step 7 — Split the process and pin
`fork()` + `socketpair(SOCK_SEQPACKET)`, pin host and device to disjoint core
sets, size the device's OpenMP team to the real device core count.

### Step 8 — Instrument the steady state
Handshake file + `perf record -p <both pids>`, 3 runs, `drop_caches` between.

### Step 9 — Emulate, measure, then move to hardware
Throttle frequency and bandwidth to the projected device parameters. Only then
port to real silicon.

---

## Part IV — Microbenchmarks to run

Run these **before** porting. They give you the constants for the break-even
model, and they tell you early if the platform is a dud.

### Platform characterization
| # | Benchmark | Tool | What it tells you |
|---|---|---|---|
| 1 | Idle load-to-use latency, local vs pool | `mlc --idle_latency`, or a pointer-chase over a shuffled array > LLC | The single most important number. Expect 2.5–4x. |
| 2 | Loaded-latency curve | `mlc --loaded_latency` | Where the link knees. Sets your safe concurrency. |
| 3 | Sequential BW, local vs pool | STREAM (already in `emulation_harness/stream/`) | Usually only 1.2–2x worse. Confirms "latency device". |
| 4 | Random 8B/64B read BW | GUPS-style, or `mlc -r` | The number that actually predicts graph performance. |
| 5 | Read vs write asymmetry on the pool | STREAM Copy vs Scale vs Triad | CXL writes are often much worse than reads. |
| 6 | NUMA topology sanity | `numactl -H`, `lstopo`, `daxctl list` | Confirm the pool is where you think it is. |

### Coherence and synchronization
| # | Benchmark | What it tells you |
|---|---|---|
| 7 | `cxl_flush_range` cost vs range size | Whether flushing whole arrays is viable or you need dirty-tracking. |
| 8 | Atomic (CAS / fetch_add) throughput: host-local, device-local, **cross-domain** | If cross-domain atomics are >10x device-local, ban them. They usually are. |
| 9 | Producer→consumer handoff latency (flush + doorbell + ack round-trip) | Your minimum offload granularity. |
| 10 | False-sharing probe: two writers on the same pool cache line | Confirms line size (**128B on some ARM**, not 64B) and the cost of getting it wrong. |

### Device compute characterization
| # | Benchmark | What it tells you |
|---|---|---|
| 11 | Same scalar kernel on host core vs one ARM core, both on **local** memory | The pure core-strength ratio. Typically 2–4x. |
| 12 | Same kernel, N ARM cores, scaling curve | Where the device's own memory system saturates. |
| 13 | Your candidate offload kernel: host-on-pool vs device-on-pool | **The offload decision, directly measured.** |
| 14 | Command round-trip with a no-op opcode | Pure protocol overhead; subtract it from #13. |

### The break-even you are computing
```
  offload wins when:
     T_host_local  +  bytes_streamed_over_link / BW_link
   > T_device_local x core_strength_ratio  +  handoff_latency x iterations
```
Benchmarks 1–5 give `BW_link`, 9/14 give `handoff_latency`, 11 gives
`core_strength_ratio`, 13 confirms the whole thing.

---

## Part V — Dos, don'ts, and sanity checks

### Do
- **Keep a non-CXL baseline in-tree** and keep it passing. Non-negotiable.
- **Put every CXL touch behind a Machine API header.** One file to swap.
- **Arena-allocate the pool.** Allocate up front, free at teardown.
- **Align pool allocations to the cache line** (64B x86, **128B on many ARM**) and
  ideally to 2 MB for hugepage-backed regions.
- **Store offsets, never pointers**, in anything the device will read.
- **One writer at a time**, with an explicit message marking the handoff.
- **Offload the low-IPC, wide-footprint phase**; keep the cache-friendly phase host-side.
- **Pin everything explicitly** — host, device, OpenMP team, perf, watcher.
- **`drop_caches` between runs**, and average ≥3 runs.
- **Instrument the steady state only.** Exclude ingest and teardown.
- **Verify placement**, don't assume it: `move_pages()` / `numastat` / `/proc/<pid>/numa_maps`.

### Don't
- ❌ **Don't `MAP_PRIVATE` the pool.** Silent COW, silently wrong answers.
- ❌ **Don't take cross-domain atomics or spinlocks over the link.** A spin loop
  polling a pool cache line from the host will melt the link. Use a doorbell.
- ❌ **Don't use `malloc`/`new` inside a device kernel.** Preallocate.
- ❌ **Don't put ingest scratch (`srcs`, `dsts`, `next_pos`) in the pool.**
  Write-once, throw-away data does not belong on the expensive path.
- ❌ **Don't assume `cxl_free` reclaims anything.** In an arena it does not.
- ❌ **Don't assume x86 memory ordering on ARM device cores.** See below.
- ❌ **Don't measure with `clock()`.** `host.c` does, and it therefore reports
  *host CPU time only* — it excludes all device work and all time blocked in
  `wait_for_device_completion`. Use `clock_gettime(CLOCK_MONOTONIC)`.
- ❌ **Don't offload without a measured break-even.** "It's near the data" is not
  a result.
- ❌ **Don't ship without an unchecked-allocation audit.** `read_graph.c` never
  checks `cxl_malloc` for `NULL`; a graph larger than the 1 GiB pool segfaults in
  a confusing place instead of reporting pool exhaustion.

### ⚠️ ARM device cores specifically
This codebase is x86-tuned. Moving `device.c` to ARM cores:
- **Weak memory model.** x86 is TSO; ARM is not. `__ATOMIC_RELAXED` in
  `atomic_min_float`'s CAS loop and in `set_bit` is *correct* for those
  individual RMWs, but any place the code relies on store→store or
  store→load ordering between different locations will break on ARM.
  Audit every relaxed atomic; add `__ATOMIC_ACQUIRE`/`__ATOMIC_RELEASE` or an
  explicit `__atomic_thread_fence` at each producer/consumer boundary.
- **Cache maintenance is explicit and privileged-ish.** `cxl_flush_range` on x86
  is `clflushopt`/`clwb` + `sfence`; on ARM it is `DC CVAC` / `DC CIVAC` loops
  plus `DSB`. Line size may be 128B — query `CTR_EL0`, don't hardcode 64.
- **Cache line size ≠ 64.** The 64-byte alignment in `cxl_malloc` and the 32-bit
  frontier words in `set_bit` both assume it. Under-alignment turns into false
  sharing across the OpenMP team.
- **No FP atomic min either**, and the CAS loop's retry cost is higher on a weaker
  core. Consider per-thread privatization + a reduction pass for `cc`/`sssp`.
- **Weaker cores, fewer of them, possibly no SIMD you expect.** Re-derive the
  offload break-even with the *real* core ratio, not the emulated one.
- **Different toolchain and different `-march`.** Don't cross-compare timings
  built with different flags — `src-c/Makefile` uses `-O2 -fno-inline` for
  profile fidelity, which is not a performance build.

### Sanity checks — run these in order
1. **Placement.** Every pool allocation actually lands on the pool.
   `move_pages()` on a sample of pages, or `numastat -p <pid>`.
2. **Sharing.** Host writes a sentinel to the pool; device reads it back and
   acks the value. Fails immediately if `MAP_SHARED` is wrong.
3. **Offset round-trip.** `pool_offset_to_ptr(base, pool_ptr_to_offset(base, p)) == p`
   on both sides, and the device's resolved pointer is *inside its own* mapping.
4. **Bounds.** Pool high-water mark vs pool size, logged at teardown. Fail loudly
   on `cxl_malloc` returning `NULL`.
5. **Bit-exact parity with the baseline.** Same graph, same algorithm, CXL build
   vs `src-c-no-shared-pool` build — identical output. For float algorithms
   (PageRank), compare within tolerance and **also** compare the iteration count;
   a differing iteration count means a real coherence bug, not FP noise.
6. **Determinism across thread counts.** `OMP_NUM_THREADS=1,2,16` must give the
   same result. If not, you have a missing atomic or a race on the mirror.
7. **Coherence stress.** Deliberately delete one `cxl_flush_range` and confirm the
   test *fails*. If it still passes, your flushes are no-ops and you have no idea
   whether you are correct.
8. **Race window.** Confirm the host never touches `vprop_mirrors` /
   `frontier_ndp` between `cxl_send_cmd` and `cxl_wait_done`. Grep the loop.
9. **Teardown.** No leaked pool region (`munmap` succeeds), device process reaped,
   socket closed. `valgrind` the host-heap side.
10. **Measurement validity.** Wall-clock, not `clock()`; steady state only;
    ≥3 runs; variance reported; caches dropped between runs.

---

## Quick reference — the seam to reimplement

| Function | Mock (`cxl_utils.c`) | Real hardware |
|---|---|---|
| `cxl_pool_init` | `mmap MAP_SHARED\|MAP_ANONYMOUS` | `mmap` DAX device / `mbind` to pool NUMA node |
| `cxl_malloc` | bump pointer, 64B aligned | arena over the pool region, line-aligned |
| `cxl_free` | no-op | no-op (arena) |
| `cxl_flush_range` | **no-op** | `clwb`+`sfence` (x86) / `DC CVAC`+`DSB` (ARM) |
| `cxl_send_cmd` | `send()` on socketpair | MMIO doorbell / submission queue |
| `cxl_wait_done` | blocking `recv()` on ack | completion queue / interrupt |
| `cxl_memcpy_to_device` | `memcpy` + flush | DMA engine, or `memcpy` + flush |
| `pool_ptr_to_offset` | `ptr - base` | unchanged — **this is why it exists** |

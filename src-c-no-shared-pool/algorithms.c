#include <float.h>
#include <limits.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "algorithms.h"
#include "utils.h"

#ifndef MAX_ITERATIONS
#define MAX_ITERATIONS 10000
#endif

#ifndef DEFAULT_NUM_THREADS
#define DEFAULT_NUM_THREADS 16
#endif

static inline void set_default_num_threads(void)
{
  omp_set_dynamic(0);
  omp_set_num_threads(DEFAULT_NUM_THREADS);
}

static inline void atomic_min_float(float* addr, float val)
{
  uint32_t* bits = (uint32_t*)addr;
  uint32_t old_bits = __atomic_load_n(bits, __ATOMIC_RELAXED);

  for (;;)
  {
    float old_val;
    memcpy(&old_val, &old_bits, sizeof(old_val));
    if (!(val < old_val))
    {
      return;
    }

    uint32_t new_bits;
    memcpy(&new_bits, &val, sizeof(new_bits));
    if (__atomic_compare_exchange_n(bits, &old_bits, new_bits, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
    {
      return;
    }
  }
}

static size_t frontier_words(const CXL_Graph* graph)
{
  return (graph->num_vertices + 31U) / 32U;
}

static void clear_mask(uint32_t* mask, size_t words)
{
#pragma omp parallel for schedule(static, 1024)
  for (size_t i = 0; i < words; ++i)
  {
    mask[i] = 0;
  }
}

static size_t count_mask_bits(const uint32_t* mask, size_t words)
{
  size_t total = 0;
  for (size_t i = 0; i < words; ++i)
  {
    total += (size_t)__builtin_popcount(mask[i]);
  }
  return total;
}

static int mask_any_set(const uint32_t* mask, size_t words)
{
  return count_mask_bits(mask, words) > 0;
}

static void fill_updates(float* updates, size_t count, float value)
{
#pragma omp parallel for schedule(static, 1024)
  for (size_t i = 0; i < count; ++i)
  {
    updates[i] = value;
  }
}

// PageRank
static void init_pagerank_state(const CXL_Graph* graph, VProp* props, uint32_t* frontier)
{
  float initial_score = 1.0f - DAMPING_FACTOR;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    set_bit(frontier, vertex);
    props[vertex].score = initial_score;
    props[vertex].delta = 0.0f;
    props[vertex].update_sum = 0.0f;
    props[vertex].pr = graph->out_degree[vertex] > 0
                           ? DAMPING_FACTOR * initial_score / (float)graph->out_degree[vertex]
                           : 0.0f;
  }
}

static void generate_updates_pagerank(
    const CXL_Graph* graph, const VProp* props, const uint32_t* frontier, float* updates)
{
  const size_t* row_ptr = graph_row_ptr(graph, 0);
  const vid_t* col_idx = graph_col_idx(graph, 0);

#pragma omp parallel for schedule(static, 1024)
  for (vid_t src = 0; src < graph->num_vertices; ++src)
  {
    if (!get_bit((uint32_t*)frontier, src))
    {
      continue;
    }

    float contribution = props[src].pr;
    if (contribution <= 0.0f)
    {
      continue;
    }

    for (size_t edge = row_ptr[src]; edge < row_ptr[src + 1]; ++edge)
    {
#pragma omp atomic update
      updates[col_idx[edge]] += contribution;
    }
  }
}

static size_t update_frontier_pagerank(const CXL_Graph* graph, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (updates[vertex] > THRESHOLD)
    {
      set_bit(next_frontier, vertex);
#pragma omp atomic update
      active_vertices++;
    }
  }

  return active_vertices;
}

static size_t apply_updates_pagerank(
    const CXL_Graph* graph, VProp* props, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (!get_bit(next_frontier, vertex))
    {
      props[vertex].delta = 0.0f;
      continue;
    }

    float update = updates[vertex];
    props[vertex].update_sum = update;
    props[vertex].score += update;
    props[vertex].delta = update;
    props[vertex].pr = graph->out_degree[vertex] > 0
                           ? DAMPING_FACTOR * update / (float)graph->out_degree[vertex]
                           : 0.0f;
#pragma omp atomic update
    active_vertices++;
  }

  return active_vertices;
}

// Connected Components
static void init_connected_components_state(const CXL_Graph* graph, VProp* props, uint32_t* frontier)
{
#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    set_bit(frontier, vertex);
    props[vertex].score = (float)vertex;
    props[vertex].delta = (float)INT_MAX;
    props[vertex].update_sum = (float)INT_MAX;
    props[vertex].pr = 0.0f;
  }
}

static void generate_updates_connected_components(
    const CXL_Graph* graph, const VProp* props, const uint32_t* frontier, float* updates)
{
  const size_t* row_ptr = graph_row_ptr(graph, 1);
  const vid_t* col_idx = graph_col_idx(graph, 1);

#pragma omp parallel for schedule(static, 1024)
  for (vid_t src = 0; src < graph->num_vertices; ++src)
  {
    if (!get_bit((uint32_t*)frontier, src))
    {
      continue;
    }

    float label = props[src].score;
    for (size_t edge = row_ptr[src]; edge < row_ptr[src + 1]; ++edge)
    {
      vid_t dst = col_idx[edge];
      atomic_min_float(&updates[dst], label);
    }
  }
}

static size_t update_frontier_connected_components(
    const CXL_Graph* graph, const VProp* props, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (updates[vertex] < props[vertex].score)
    {
      set_bit(next_frontier, vertex);
#pragma omp atomic update
      active_vertices++;
    }
  }

  return active_vertices;
}

static size_t apply_updates_connected_components(
    const CXL_Graph* graph, VProp* props, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (!get_bit(next_frontier, vertex))
    {
      props[vertex].delta = (float)INT_MAX;
      continue;
    }

    props[vertex].score = updates[vertex];
    props[vertex].update_sum = updates[vertex];
    props[vertex].delta = (float)INT_MAX;
#pragma omp atomic update
    active_vertices++;
  }

  return active_vertices;
}

// SSSP
static void init_sssp_state(const CXL_Graph* graph, VProp* props, uint32_t* frontier)
{
#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    props[vertex].score = FLT_MAX;
    props[vertex].delta = FLT_MAX;
    props[vertex].update_sum = FLT_MAX;
    props[vertex].pr = 0.0f;
  }

  if (graph->num_vertices > 0)
  {
    set_bit(frontier, 0);
    props[0].score = 0.0f;
    props[0].delta = 0.0f;
    props[0].update_sum = 0.0f;
  }
}

static void generate_updates_sssp(
    const CXL_Graph* graph, const VProp* props, const uint32_t* frontier, float* updates)
{
  const size_t* row_ptr = graph_row_ptr(graph, 1);
  const vid_t* col_idx = graph_col_idx(graph, 1);

#pragma omp parallel for schedule(static, 1024)
  for (vid_t src = 0; src < graph->num_vertices; ++src)
  {
    if (!get_bit((uint32_t*)frontier, src))
    {
      continue;
    }

    if (props[src].score == FLT_MAX)
    {
      continue;
    }

    float candidate = props[src].score + 1.0f;
    for (size_t edge = row_ptr[src]; edge < row_ptr[src + 1]; ++edge)
    {
      vid_t dst = col_idx[edge];
      atomic_min_float(&updates[dst], candidate);
    }
  }
}

static size_t update_frontier_sssp(const CXL_Graph* graph, const VProp* props, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (updates[vertex] < props[vertex].score)
    {
      set_bit(next_frontier, vertex);
#pragma omp atomic update
      active_vertices++;
    }
  }

  return active_vertices;
}

static size_t apply_updates_sssp(const CXL_Graph* graph, VProp* props, const float* updates, uint32_t* next_frontier)
{
  size_t active_vertices = 0;

#pragma omp parallel for schedule(static, 1024)
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    if (!get_bit(next_frontier, vertex))
    {
      props[vertex].delta = FLT_MAX;
      continue;
    }

    props[vertex].score = updates[vertex];
    props[vertex].update_sum = updates[vertex];
    props[vertex].delta = updates[vertex];
#pragma omp atomic update
    active_vertices++;
  }

  return active_vertices;
}

size_t run_pagerank(const CXL_Graph* graph, VProp* props)
{
  set_default_num_threads();
  size_t words = frontier_words(graph);
  uint32_t* frontier_current = (uint32_t*)calloc(words, sizeof(uint32_t));
  uint32_t* frontier_next = (uint32_t*)calloc(words, sizeof(uint32_t));
  float* updates = (float*)calloc(graph->num_vertices, sizeof(float));

  if (!frontier_current || !frontier_next || !updates)
  {
    free(frontier_current);
    free(frontier_next);
    free(updates);
    return 0;
  }

  init_pagerank_state(graph, props, frontier_current);
  if (profiling_enabled())
  {
    write_measurement_flag(1);
    wait_for_profiler_ack(30000); // wait up to 30s for profiler to attach
    sleep(2);
  }

  size_t iterations = 0;
  for (; iterations < MAX_ITERATIONS; ++iterations)
  {
    if (!mask_any_set(frontier_current, words))
    {
      break;
    }

    clear_mask(frontier_next, words);
    fill_updates(updates, graph->num_vertices, 0.0f);

    generate_updates_pagerank(graph, props, frontier_current, updates);
    size_t active_vertices = update_frontier_pagerank(graph, updates, frontier_next);
    if (active_vertices == 0)
    {
      break;
    }

    (void)apply_updates_pagerank(graph, props, updates, frontier_next);

    uint32_t* swap = frontier_current;
    frontier_current = frontier_next;
    frontier_next = swap;
  }

  if (profiling_enabled())
  {
    write_measurement_flag(0);
  }
  free(frontier_current);
  free(frontier_next);
  free(updates);
  print_pagerank_summary(graph, props);
  return iterations;
}

size_t run_connected_components(const CXL_Graph* graph, VProp* props)
{
  set_default_num_threads();
  size_t words = frontier_words(graph);
  uint32_t* frontier_current = (uint32_t*)calloc(words, sizeof(uint32_t));
  uint32_t* frontier_next = (uint32_t*)calloc(words, sizeof(uint32_t));
  float* updates = (float*)calloc(graph->num_vertices, sizeof(float));

  if (!frontier_current || !frontier_next || !updates)
  {
    free(frontier_current);
    free(frontier_next);
    free(updates);
    return 0;
  }

  init_connected_components_state(graph, props, frontier_current);
  if (profiling_enabled())
  {
    write_measurement_flag(1);
    wait_for_profiler_ack(30000);
    sleep(2);
  }

  size_t iterations = 0;
  for (; iterations < MAX_ITERATIONS; ++iterations)
  {
    if (!mask_any_set(frontier_current, words))
    {
      break;
    }

    clear_mask(frontier_next, words);
    fill_updates(updates, graph->num_vertices, (float)INT_MAX);

    generate_updates_connected_components(graph, props, frontier_current, updates);
    size_t active_vertices = update_frontier_connected_components(graph, props, updates, frontier_next);
    if (active_vertices == 0)
    {
      break;
    }

    (void)apply_updates_connected_components(graph, props, updates, frontier_next);

    uint32_t* swap = frontier_current;
    frontier_current = frontier_next;
    frontier_next = swap;
  }

  if (profiling_enabled())
  {
    write_measurement_flag(0);
  }
  free(frontier_current);
  free(frontier_next);
  free(updates);
  print_connected_components_summary(graph, props);
  return iterations;
}

size_t run_sssp(const CXL_Graph* graph, VProp* props)
{
  set_default_num_threads();
  size_t words = frontier_words(graph);
  uint32_t* frontier_current = (uint32_t*)calloc(words, sizeof(uint32_t));
  uint32_t* frontier_next = (uint32_t*)calloc(words, sizeof(uint32_t));
  float* updates = (float*)calloc(graph->num_vertices, sizeof(float));

  if (!frontier_current || !frontier_next || !updates)
  {
    free(frontier_current);
    free(frontier_next);
    free(updates);
    return 0;
  }

  init_sssp_state(graph, props, frontier_current);
  if (profiling_enabled())
  {
    write_measurement_flag(1);
    wait_for_profiler_ack(30000);
    sleep(2);
  }

  size_t iterations = 0;
  for (; iterations < MAX_ITERATIONS; ++iterations)
  {
    if (!mask_any_set(frontier_current, words))
    {
      break;
    }

    clear_mask(frontier_next, words);
    fill_updates(updates, graph->num_vertices, FLT_MAX);

    generate_updates_sssp(graph, props, frontier_current, updates);
    size_t active_vertices = update_frontier_sssp(graph, props, updates, frontier_next);
    if (active_vertices == 0)
    {
      break;
    }

    (void)apply_updates_sssp(graph, props, updates, frontier_next);

    uint32_t* swap = frontier_current;
    frontier_current = frontier_next;
    frontier_next = swap;
  }

  if (profiling_enabled())
  {
    write_measurement_flag(0);
  }
  free(frontier_current);
  free(frontier_next);
  free(updates);
  print_sssp_summary(graph, props);
  return iterations;
}

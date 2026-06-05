#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/select.h>
#include <float.h>

#include "utils.h"

#define MEASUREMENT_FILE "./measurement"

int profiling_enabled(void)
{
  const char* enabled = getenv("GRUDON_PROFILE");
  return enabled && strcmp(enabled, "1") == 0;
}

void write_measurement_flag(int flag)
{
  FILE* file = fopen(MEASUREMENT_FILE, "w");
  if (!file)
  {
    perror("fopen(measurement)");
    return;
  }

  fprintf(file, "%d\n%ld\n", flag, (long)getpid());
  fclose(file);
}

void wait_for_profiler_ack(unsigned timeout_ms)
{
  if (!profiling_enabled())
  {
    return;
  }
  const char* ack = "./measurement.attached";
  unsigned waited = 0;
  const unsigned sleep_ms = 1;
  while (waited < timeout_ms)
  {
    if (access(ack, F_OK) == 0)
    {
      return;
    }
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = (long)sleep_ms * 1000L;
    select(0, NULL, NULL, NULL, &tv);
    waited += sleep_ms;
  }
  fprintf(stderr, "Warning: profiler ack not received within %u ms, proceeding anyway\n", timeout_ms);
}

static int compare_rank_pairs(const void* lhs, const void* rhs)
{
  const RankPair* left = (const RankPair*)lhs;
  const RankPair* right = (const RankPair*)rhs;

  if (left->score < right->score)
  {
    return 1;
  }
  if (left->score > right->score)
  {
    return -1;
  }
  if (left->vertex_id < right->vertex_id)
  {
    return -1;
  }
  if (left->vertex_id > right->vertex_id)
  {
    return 1;
  }
  return 0;
}

void print_pagerank_summary(const CXL_Graph* graph, const VProp* props)
{
  RankPair* ranks = (RankPair*)malloc(graph->num_vertices * sizeof(RankPair));
  if (!ranks)
  {
    perror("malloc");
    return;
  }

  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    ranks[vertex].vertex_id = vertex;
    ranks[vertex].score = props[vertex].score;
  }

  qsort(ranks, graph->num_vertices, sizeof(RankPair), compare_rank_pairs);

  size_t top_count = graph->num_vertices < 10 ? graph->num_vertices : 10;
  printf("\n=== Top %zu PageRank Vertices ===\n", top_count);
  for (size_t index = 0; index < top_count; ++index)
  {
    printf("%zu: vertex %u score %.6f\n", index + 1, ranks[index].vertex_id, ranks[index].score);
  }

  free(ranks);
}

void print_connected_components_summary(const CXL_Graph* graph, const VProp* props)
{
  size_t* seen = (size_t*)calloc(graph->num_vertices, sizeof(size_t));
  if (!seen)
  {
    perror("calloc");
    return;
  }

  size_t components = 0;
  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    size_t component_id = (size_t)props[vertex].score;
    if (component_id < graph->num_vertices && seen[component_id] == 0)
    {
      seen[component_id] = 1;
      components++;
    }
  }

  printf("\n=== Connected Components ===\n");
  printf("Total connected components: %zu\n", components);
  free(seen);
}

void print_sssp_summary(const CXL_Graph* graph, const VProp* props)
{
  size_t reachable = 0;
  float max_distance = 0.0f;

  for (vid_t vertex = 0; vertex < graph->num_vertices; ++vertex)
  {
    float distance = props[vertex].score;
    if (distance < FLT_MAX)
    {
      reachable++;
      if (distance > max_distance)
      {
        max_distance = distance;
      }
    }
  }

  printf("\n=== SSSP Summary ===\n");
  printf("Reachable vertices: %zu / %u\n", reachable, graph->num_vertices);
  printf("Maximum distance: %.6f\n", max_distance);
}

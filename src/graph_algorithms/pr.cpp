#include "graph_algorithms/pr.hpp"

#include <cstdlib>
#include <map>

#include "RuntimeProfiler.hpp"

namespace {

[[nodiscard]] bool parseEnvFlag(const char* name, bool fallback)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
  {
    return fallback;
  }

  const std::string normalized(value);
  if (normalized == "0" || normalized == "false" || normalized == "False" || normalized == "FALSE")
  {
    return false;
  }

  return true;
}

}  // namespace

template<typename VertexProperty>
PageRank<VertexProperty>::PageRank(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net,
    std::string partitioning_scheme_file)
    : GraphAlgorithm<VertexProperty>(node_type, algorithm_name, graph_path, num_compute, num_memory, node_id, net, partitioning_scheme_file)
{
  this->algorithm_name = "PageRank";
  this->pr_internal_profile_enabled_ = parseEnvFlag("GRUDON_ENABLE_PR_INTERNAL_PROFILE", false);
  this->pr_fine_profile_enabled_ = parseEnvFlag("GRUDON_ENABLE_PR_FINE_PROFILE", false);
}

template<typename VertexProperty>
void PageRank<VertexProperty>::init()
{
  // TODO: Try the std::distance() - 1 for socLJ 120 Iterations --> 150 Iterations
  // Initialize the Vertex Properties
  if (this->node_type == COMPUTE_NODE)
  {
    this->pr_vals.allocate(this->worker->num_vertices);
    this->prev_updates.allocate(this->worker->num_vertices);

    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      if (this->worker->out_degrees[n] != 0)
      {
        this->vertex_properties[n] = DAMPING_FACTOR * (1.0 - DAMPING_FACTOR) / this->worker->out_degrees[n];
        this->pr_vals[n] = 1.0 - DAMPING_FACTOR;
        // this->frontier.push_back(n);
        this->frontier.set(n);

        this->vertex_updates[n] = 0.0;
        this->prev_updates[n] = 0.0;
      }
      else
      {
        this->vertex_properties[n] = 0.0;
        this->pr_vals[n] = 0.0;
        this->vertex_updates[n] = 0.0;
        this->prev_updates[n] = 0.0;
      }
    }
  }
  else if (this->node_type == MEMORY_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      this->vertex_properties[n] = 0.0;
      this->vertex_updates[n] = 0.0;
    }
  }

  this->clear_updates = true;
}

template<typename VertexProperty>
void PageRank<VertexProperty>::apply_updates()
{
  // Print the Vertex Properties
  // for (GNode n = 0; n < this->worker->num_vertices; ++n)
  // {
  //   spdlog::info("[Proc {}] Vertex {}: {}/{}", this->worker->node_id, n, this->pr_vals[n], this->vertex_updates[n]);
  // }
  if (!this->pr_internal_profile_enabled_ || this->runtime_profiler_ == nullptr ||
      !this->runtime_profiler_->hasOperation("pr_apply_collect_frontier"))
  {
    std::vector<GNode> frontier_iter = this->frontier.getOffsets();
    galois::do_all(
        galois::iterate(frontier_iter),
        [&](GNode lid)
        {
          VertexProperty update_val = this->vertex_updates[lid];
          if (update_val > TOLERANCE)
          {
            this->pr_vals.addUpdate(lid, update_val);
            this->vertex_properties[lid] = DAMPING_FACTOR * update_val / this->worker->out_degrees[lid];
            this->vertex_updates[lid] = 0.0;
            this->prev_updates[lid] = 0.0;
          }
        },
        galois::loopname("Apply Updates"),
        galois::no_stats(),
        galois::steal());
    return;
  }

  RuntimeProfiler& profiler = *this->runtime_profiler_;

  if (this->pr_fine_profile_enabled_ && this->runtime_profiler_->hasOperation("pr_apply_load_active_updates"))
  {
    std::vector<GNode> frontier_iter;
    galois::DynamicBitSet apply_mask;
    std::vector<GNode> active_vertices;
    std::vector<VertexProperty> active_updates;

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_collect_frontier");
      frontier_iter = this->frontier.getOffsets();
      apply_mask.resize(this->worker->num_vertices);
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_filter_updates");
      galois::do_all(
          galois::iterate(frontier_iter),
          [&](GNode lid)
          {
            if (this->vertex_updates[lid] > TOLERANCE)
            {
              apply_mask.set(lid);
            }
          },
          galois::loopname("PR Apply Filter Updates"),
          galois::no_stats(),
          galois::steal());
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_load_active_updates");
      active_vertices = apply_mask.getOffsets();
      active_updates.resize(active_vertices.size(), static_cast<VertexProperty>(0));
      galois::do_all(
          galois::iterate(size_t(0), active_vertices.size()),
          [&](size_t idx)
          {
            active_updates[idx] = this->vertex_updates[active_vertices[idx]];
          },
          galois::loopname("PR Apply Load Active Updates"),
          galois::no_stats(),
          galois::steal());
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_accumulate_pr");
      galois::do_all(
          galois::iterate(size_t(0), active_vertices.size()),
          [&](size_t idx)
          {
            this->pr_vals.addUpdate(active_vertices[idx], active_updates[idx]);
          },
          galois::loopname("PR Apply Accumulate PR"),
          galois::no_stats(),
          galois::steal());
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_recompute_property");
      galois::do_all(
          galois::iterate(size_t(0), active_vertices.size()),
          [&](size_t idx)
          {
            const GNode lid = active_vertices[idx];
            const uint64_t out_degree = this->worker->out_degrees[lid];
            if (out_degree > 0)
            {
              this->vertex_properties[lid] = DAMPING_FACTOR * active_updates[idx] / out_degree;
            }
          },
          galois::loopname("PR Apply Recompute Property"),
          galois::no_stats(),
          galois::steal());
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_apply_clear_buffers");
      galois::do_all(
          galois::iterate(active_vertices),
          [&](GNode lid)
          {
            this->vertex_updates[lid] = 0.0;
            this->prev_updates[lid] = 0.0;
          },
          galois::loopname("PR Apply Clear Buffers"),
          galois::no_stats(),
          galois::steal());
    }

    return;
  }

  std::vector<GNode> frontier_iter;
  galois::DynamicBitSet apply_mask;

  {
    ScopedOperationProfile profile_scope(profiler, "pr_apply_collect_frontier");
    frontier_iter = this->frontier.getOffsets();
    apply_mask.resize(this->worker->num_vertices);
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_apply_filter_updates");
    galois::do_all(
        galois::iterate(frontier_iter),
        [&](GNode lid)
        {
          if (this->vertex_updates[lid] > TOLERANCE)
          {
            apply_mask.set(lid);
          }
        },
        galois::loopname("PR Apply Filter Updates"),
        galois::no_stats(),
        galois::steal());
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_apply_commit_updates");
    const std::vector<GNode> active_vertices = apply_mask.getOffsets();
    galois::do_all(
        galois::iterate(active_vertices),
        [&](GNode lid)
        {
          const VertexProperty update_val = this->vertex_updates[lid];
          this->pr_vals.addUpdate(lid, update_val);
          this->vertex_properties[lid] = DAMPING_FACTOR * update_val / this->worker->out_degrees[lid];
          this->vertex_updates[lid] = 0.0;
          this->prev_updates[lid] = 0.0;
        },
        galois::loopname("PR Apply Commit Updates"),
        galois::no_stats(),
        galois::steal());
  }
}

template<typename VertexProperty>
void PageRank<VertexProperty>::gen_updates()
{
  if (!this->pr_internal_profile_enabled_ || this->runtime_profiler_ == nullptr ||
      !this->runtime_profiler_->hasOperation("pr_gen_collect_sources"))
  {
    // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_properties.getUpdatedVertices();
    std::vector<GNode> updated_vertices = this->vertex_properties.getUpdatedVertices();
    galois::do_all(
        galois::iterate(updated_vertices.begin(), updated_vertices.end()),
        [&](GNode lid)
        {
          auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
          auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
          VertexProperty pr_val = this->vertex_properties[lid];

          for (; ii != ei; ++ii)
          {
            GNode dst = this->worker->distributed_graph->lgraph.getEdgeDst(ii);

            // spdlog::info(
            //     "[Proc {}] Edge {} -> {}: {} + {}",
            //     this->worker->node_id,
            //     lid,
            //     this->worker->distributed_graph->lgraph.getEdgeDst(ii),
            //     this->vertex_properties[lid],
            //     this->vertex_updates[dst]);

            this->vertex_updates.addUpdate(dst, pr_val);
          }
        },
        galois::loopname("Generate Updates"),
        galois::no_stats(),
        galois::steal());
    return;
  }

  RuntimeProfiler& profiler = *this->runtime_profiler_;

  if (this->pr_fine_profile_enabled_ && this->runtime_profiler_->hasOperation("pr_gen_count_edges"))
  {
    std::vector<GNode> updated_vertices;
    std::vector<std::pair<GNode, VertexProperty>> edge_contribs;

    {
      ScopedOperationProfile profile_scope(profiler, "pr_gen_collect_sources");
      updated_vertices = this->vertex_properties.getUpdatedVertices();
    }

    size_t total_edges = 0;
    {
      ScopedOperationProfile profile_scope(profiler, "pr_gen_count_edges");
      for (const GNode lid : updated_vertices)
      {
        auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
        auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
        total_edges += std::distance(ii, ei);
      }
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_gen_expand_edge_contribs");
      edge_contribs.reserve(total_edges);
      for (const GNode lid : updated_vertices)
      {
        auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
        auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
        const VertexProperty pr_val = this->vertex_properties[lid];
        for (; ii != ei; ++ii)
        {
          edge_contribs.emplace_back(this->worker->distributed_graph->lgraph.getEdgeDst(ii), pr_val);
        }
      }
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_gen_scatter_updates");
      galois::do_all(
          galois::iterate(size_t(0), edge_contribs.size()),
          [&](size_t idx)
          {
            const auto& [dst, pr_val] = edge_contribs[idx];
            this->vertex_updates.addUpdate(dst, pr_val);
          },
          galois::loopname("PR Gen Scatter Updates"),
          galois::no_stats(),
          galois::steal());
    }

    return;
  }

  std::vector<GNode> updated_vertices;
  std::vector<std::pair<GNode, VertexProperty>> edge_contribs;

  {
    ScopedOperationProfile profile_scope(profiler, "pr_gen_collect_sources");
    updated_vertices = this->vertex_properties.getUpdatedVertices();
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_gen_collect_edge_contribs");
    size_t total_edges = 0;
    for (const GNode lid : updated_vertices)
    {
      auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
      auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
      total_edges += std::distance(ii, ei);
    }

    edge_contribs.reserve(total_edges);
    for (const GNode lid : updated_vertices)
    {
      auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
      auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
      const VertexProperty pr_val = this->vertex_properties[lid];
      for (; ii != ei; ++ii)
      {
        edge_contribs.emplace_back(this->worker->distributed_graph->lgraph.getEdgeDst(ii), pr_val);
      }
    }
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_gen_scatter_updates");
    galois::do_all(
        galois::iterate(size_t(0), edge_contribs.size()),
        [&](size_t idx)
        {
          const auto& [dst, pr_val] = edge_contribs[idx];
          this->vertex_updates.addUpdate(dst, pr_val);
        },
        galois::loopname("PR Gen Scatter Updates"),
        galois::no_stats(),
        galois::steal());
  }
}

template<typename VertexProperty>
void PageRank<VertexProperty>::update_frontier()
{
  // galois::substrate::SimpleLock lock;
  // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_updates.getUpdatedVertices();
  if (!this->pr_internal_profile_enabled_ || this->runtime_profiler_ == nullptr ||
      !this->runtime_profiler_->hasOperation("pr_frontier_collect_candidates"))
  {
    std::vector<GNode> updated_vertices = this->vertex_updates.getUpdatedVertices();
    galois::do_all(
        galois::iterate(updated_vertices.begin(), updated_vertices.end()),
        [&](GNode lid)
        {
          VertexProperty update_val = this->vertex_updates[lid];
          if (update_val > TOLERANCE && this->prev_updates[lid] < TOLERANCE)
          {
            // lock.lock();
            // this->frontier.push_back(lid);
            this->frontier.set(lid);
            // lock.unlock();

            this->prev_updates[lid] = update_val;
          }
        },
        galois::loopname("Update Frontier"),
        galois::no_stats(),
        galois::steal());
    return;
  }

  RuntimeProfiler& profiler = *this->runtime_profiler_;

  if (this->pr_fine_profile_enabled_ && this->runtime_profiler_->hasOperation("pr_frontier_set_bits"))
  {
    std::vector<GNode> updated_vertices;
    galois::DynamicBitSet active_candidates;
    std::vector<GNode> active_vertices;

    {
      ScopedOperationProfile profile_scope(profiler, "pr_frontier_collect_candidates");
      updated_vertices = this->vertex_updates.getUpdatedVertices();
      active_candidates.resize(this->worker->num_vertices);
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_frontier_select_active");
      galois::do_all(
          galois::iterate(updated_vertices.begin(), updated_vertices.end()),
          [&](GNode lid)
          {
            const VertexProperty update_val = this->vertex_updates[lid];
            if (update_val > TOLERANCE && this->prev_updates[lid] < TOLERANCE)
            {
              active_candidates.set(lid);
            }
          },
          galois::loopname("PR Frontier Select Active"),
          galois::no_stats(),
          galois::steal());
    }

    active_vertices = active_candidates.getOffsets();

    {
      ScopedOperationProfile profile_scope(profiler, "pr_frontier_set_bits");
      galois::do_all(
          galois::iterate(active_vertices.begin(), active_vertices.end()),
          [&](GNode lid)
          {
            this->frontier.set(lid);
          },
          galois::loopname("PR Frontier Set Bits"),
          galois::no_stats(),
          galois::steal());
    }

    {
      ScopedOperationProfile profile_scope(profiler, "pr_frontier_store_prev_updates");
      galois::do_all(
          galois::iterate(active_vertices.begin(), active_vertices.end()),
          [&](GNode lid)
          {
            this->prev_updates[lid] = this->vertex_updates[lid];
          },
          galois::loopname("PR Frontier Store Prev"),
          galois::no_stats(),
          galois::steal());
    }

    return;
  }

  std::vector<GNode> updated_vertices;
  galois::DynamicBitSet active_candidates;

  {
    ScopedOperationProfile profile_scope(profiler, "pr_frontier_collect_candidates");
    updated_vertices = this->vertex_updates.getUpdatedVertices();
    active_candidates.resize(this->worker->num_vertices);
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_frontier_select_active");
    galois::do_all(
        galois::iterate(updated_vertices.begin(), updated_vertices.end()),
        [&](GNode lid)
        {
          const VertexProperty update_val = this->vertex_updates[lid];
          if (update_val > TOLERANCE && this->prev_updates[lid] < TOLERANCE)
          {
            active_candidates.set(lid);
          }
        },
        galois::loopname("PR Frontier Select Active"),
        galois::no_stats(),
        galois::steal());
  }

  {
    ScopedOperationProfile profile_scope(profiler, "pr_frontier_commit_active");
    const std::vector<GNode> active_vertices = active_candidates.getOffsets();
    galois::do_all(
        galois::iterate(active_vertices.begin(), active_vertices.end()),
        [&](GNode lid)
        {
          this->frontier.set(lid);
          this->prev_updates[lid] = this->vertex_updates[lid];
        },
        galois::loopname("PR Frontier Commit Active"),
        galois::no_stats(),
        galois::steal());
  }
}

template<typename VertexProperty>
void PageRank<VertexProperty>::aggregate(GNode &lid, const VertexProperty &buffer_val)
{
  this->vertex_updates.addUpdate(lid, buffer_val);
}

template<typename VertexProperty>
bool PageRank<VertexProperty>::termination_check()
{
  // return this->frontier.empty();
  return this->frontier.count() ? false : true;
}

template<typename VertexProperty>
void PageRank<VertexProperty>::printState()
{
  if (this->worker->node_type == COMPUTE_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      spdlog::info(
          "[Proc {}] Vertex/PR: {}/{}",
          this->worker->node_id,
          this->worker->distributed_graph->getGlobalNode(n),
          this->pr_vals[n]);
    }
  }
}

template<typename VertexProperty>
void PageRank<VertexProperty>::verify()
{
  // Create a map of PageRank values to the Global Node ID
  // Print the top 20 PageRank values

  if (this->worker->node_type == MEMORY_NODE)
  {
    return;
  }

  std::map<VertexProperty, GNode> pr_map;
  for (GNode n = 0; n < this->worker->num_vertices; ++n)
  {
    pr_map[this->pr_vals[n]] = this->worker->distributed_graph->getGlobalNode(n);
  }

  spdlog::info("Top 20 PageRank Values:");
  int count = 0;
  for (auto it = pr_map.rbegin(); it != pr_map.rend(); ++it)
  {
    spdlog::info("Node: {}, PR: {}", it->second, it->first);
    count++;
    if (count == 20)
    {
      break;
    }
  }
}
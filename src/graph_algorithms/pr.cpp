#include "graph_algorithms/pr.hpp"

#include <map>

template<typename VertexProperty>
PageRank<VertexProperty>::PageRank(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net)
    : GraphAlgorithm<VertexProperty>(node_type, algorithm_name, graph_path, num_compute, num_memory, node_id, net)
{
  this->algorithm_name = "PageRank";
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
}

template<typename VertexProperty>
void PageRank<VertexProperty>::gen_updates()
{
  // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_properties.getUpdatedVertices();
  std::vector<unsigned int> updated_vertices = this->vertex_properties.getUpdatedVertices();
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
}

template<typename VertexProperty>
void PageRank<VertexProperty>::update_frontier()
{
  // galois::substrate::SimpleLock lock;
  // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_updates.getUpdatedVertices();
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
  std::vector<GNode> frontier_iter = this->frontier.getOffsets();
  spdlog::debug("[Proc {}] Frontier: {}", this->worker->node_id, fmt_array(frontier_iter));
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
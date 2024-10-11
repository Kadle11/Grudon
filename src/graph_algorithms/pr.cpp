#include "graph_algorithms/pr.hpp"

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
        this->frontier.push_back(n);

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
}

template<typename VertexProperty>
void PageRank<VertexProperty>::apply_updates()
{
  // Print the Vertex Properties
  // for (GNode n = 0; n < this->worker->num_vertices; ++n)
  // {
  //   spdlog::info("[Proc {}] Vertex {}: {}/{}", this->worker->node_id, n, this->pr_vals[n], this->vertex_updates[n]);
  // }

  galois::do_all(
      galois::iterate(this->frontier),
      [&](GNode lid)
      {
        if (this->vertex_updates[lid] > TOLERANCE)
        {
          this->pr_vals.addUpdate(lid, this->vertex_updates[lid]);
          this->vertex_properties[lid] = DAMPING_FACTOR * this->vertex_updates[lid] / this->worker->out_degrees[lid];
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
  // FIXME: DST == 10 when N > 2
  const std::set<GNode> &updated_vertices = this->vertex_properties.getUpdatedVertices();
  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
        auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
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

          this->vertex_updates.addUpdate(dst, this->vertex_properties[lid]);
        }
      },
      galois::loopname("Generate Updates"),
      galois::no_stats(),
      galois::steal());
}

template<typename VertexProperty>
void PageRank<VertexProperty>::update_frontier()
{
  galois::substrate::SimpleLock lock;
  const std::set<GNode> &updated_vertices = this->vertex_updates.getUpdatedVertices();
  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        if (this->vertex_updates[lid] > TOLERANCE && this->prev_updates[lid] < TOLERANCE)
        {
          lock.lock();
          this->frontier.push_back(lid);
          lock.unlock();

          this->prev_updates[lid] = this->vertex_updates[lid];
        }
      },
      galois::loopname("Update Frontier"),
      galois::no_stats(),
      galois::steal());

  spdlog::debug("[Proc {}] Frontier: {}", this->worker->node_id, fmt_array(this->frontier));
}

template<typename VertexProperty>
void PageRank<VertexProperty>::aggregate(GNode &lid, const VertexProperty &buffer_val)
{
  this->vertex_updates.addUpdate(lid, buffer_val);
}

template<typename VertexProperty>
bool PageRank<VertexProperty>::termination_check()
{
  return this->frontier.empty();
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
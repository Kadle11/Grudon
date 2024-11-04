#include "graph_algorithms/sssp.hpp"

template<typename VertexProperty>
SSSP<VertexProperty>::SSSP(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net)
    : GraphAlgorithm<VertexProperty>(node_type, algorithm_name, graph_path, num_compute, num_memory, node_id, net)
{
  this->algorithm_name = "SSSP";
}

template<typename VertexProperty>
void SSSP<VertexProperty>::init()
{
  // TODO: T
  // Initialize the Vertex Properties
  if (this->node_type == COMPUTE_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      this->vertex_properties[n] = std::numeric_limits<VertexProperty>::max();
      this->vertex_updates[n] = std::numeric_limits<VertexProperty>::max();
    }
  }
  else if (this->node_type == MEMORY_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      this->vertex_properties[n] = std::numeric_limits<VertexProperty>::max();
      this->vertex_updates[n] = std::numeric_limits<VertexProperty>::max();
    }
  }

  // Initialize the Source Node
  if (this->worker->node_id == 0)
  {
    this->vertex_properties[0] = 0;
    this->vertex_updates[0] = 0;
    // this->frontier.push_back(0);
    this->frontier.set(0);
  }
}

template<typename VertexProperty>
void SSSP<VertexProperty>::apply_updates()
{
  // Print the Vertex Properties
  // for (GNode n = 0; n < this->worker->num_vertices; ++n)
  // {
  //   spdlog::info(
  //       "[Proc {}] Vertex {}: {}/{}", this->worker->node_id, n, this->vertex_properties[n], this->vertex_updates[n]);
  // }
}

template<typename VertexProperty>
void SSSP<VertexProperty>::gen_updates()
{
  // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_properties.getUpdatedVertices();
  std::vector<GNode> updated_vertices = this->vertex_updates.getUpdatedVertices();
  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        VertexProperty n_dist = this->vertex_properties[lid] + 1;
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

          this->vertex_updates.minUpdate(dst, n_dist);
        }
      },
      galois::loopname("Generate Updates"),
      galois::no_stats(),
      galois::steal());
}

template<typename VertexProperty>
void SSSP<VertexProperty>::update_frontier()
{
  galois::substrate::SimpleLock lock;
  // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_updates.getUpdatedVertices();
  std::vector<GNode> updated_vertices = this->vertex_updates.getUpdatedVertices();
  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        if (this->vertex_properties[lid] > this->vertex_updates[lid])
        {
          this->vertex_properties[lid] = this->vertex_updates[lid];
          // this->frontier.push_back(lid);
          this->frontier.set(lid);
        }
      },
      galois::loopname("Update Frontier"),
      galois::no_stats(),
      galois::steal());
  std::vector<GNode> frontier_iter = this->frontier.getOffsets();
  spdlog::debug("[Proc {}] Frontier: {}", this->worker->node_id, fmt_array(frontier_iter));
}

template<typename VertexProperty>
void SSSP<VertexProperty>::aggregate(GNode &lid, const VertexProperty &buffer_val)
{
  this->vertex_updates.minUpdate(lid, buffer_val);
}

template<typename VertexProperty>
bool SSSP<VertexProperty>::termination_check()
{
  // return this->frontier.empty();
  return this->frontier.count() ? false : true;
}

template<typename VertexProperty>
void SSSP<VertexProperty>::printState()
{
  if (this->worker->node_type == COMPUTE_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      spdlog::info(
          "[Proc {}] Vertex/Dist: {}/{}",
          this->worker->node_id,
          this->worker->distributed_graph->getGlobalNode(n),
          this->vertex_properties[n]);
    }
  }
}
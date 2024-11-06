#include "graph_algorithms/cc.hpp"

template<typename VertexProperty>
CC<VertexProperty>::CC(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net)
    : GraphAlgorithm<VertexProperty>(node_type, algorithm_name, graph_path, num_compute, num_memory, node_id, net)
{
  this->algorithm_name = "CC";
}

template<typename VertexProperty>
void CC<VertexProperty>::init()
{
  // Initialize the Vertex Properties
  if (this->node_type == COMPUTE_NODE)
  {
    for (GNode n = 0; n < this->worker->num_vertices; ++n)
    {
      this->vertex_properties[n] = this->worker->distributed_graph->getGlobalNode(n);
      this->vertex_updates[n] = std::numeric_limits<VertexProperty>::max();
      this->frontier.push_back(n);
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
}

template<typename VertexProperty>
void CC<VertexProperty>::apply_updates()
{
  // // Print the Vertex Properties
  // for (GNode n = 0; n < this->worker->num_vertices; ++n)
  // {
  //   spdlog::info(
  //       "[Proc {}] Vertex {}: {}/{}", this->worker->node_id, n, this->vertex_properties[n], this->vertex_updates[n]);
  // }
}

template<typename VertexProperty>
void CC<VertexProperty>::gen_updates()
{
  galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_properties.getUpdatedVertices();
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

          this->vertex_updates.minUpdate(dst, this->vertex_properties[lid]);
        }
      },
      galois::loopname("Generate Updates"),
      galois::no_stats(),
      galois::steal());
}

template<typename VertexProperty>
void CC<VertexProperty>::update_frontier()
{
  galois::substrate::SimpleLock lock;
  galois::ThreadSafeOrderedSet<GNode> &updated_vertices = this->vertex_updates.getUpdatedVertices();
  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        if (this->vertex_properties[lid] > this->vertex_updates[lid])
        {
          this->vertex_properties[lid] = this->vertex_updates[lid];
          this->frontier.push_back(lid);
        }
      },
      galois::loopname("Update Frontier"),
      galois::no_stats(),
      galois::steal());

  spdlog::info("[Proc {}] Frontier: {}", this->worker->node_id, fmt_array(this->frontier));
}

template<typename VertexProperty>
void CC<VertexProperty>::aggregate(GNode &lid, const VertexProperty &buffer_val)
{
  this->vertex_updates.minUpdate(lid, buffer_val);
}

template<typename VertexProperty>
bool CC<VertexProperty>::termination_check()
{
  return this->frontier.empty();
}

template<typename VertexProperty>
void CC<VertexProperty>::printState()
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

template<typename VertexProperty>
void CC<VertexProperty>::verify()
{
  // Verify that the neighbors have the same component

  if (this->worker->node_type == MEMORY_NODE)
  {
    return;
  }

  Graph vGraph;
  galois::graphs::readGraph(vGraph, this->graph_path);

  galois::do_all(
      galois::iterate(0ul, this->worker->num_vertices),
      [&](GNode gid)
      {
        auto ii = vGraph.edge_begin(gid);
        auto ei = vGraph.edge_end(gid);

        for (; ii != ei; ++ii)
        {
          GNode dst = vGraph.getEdgeDst(ii);

          if (this->vertex_properties[gid] != this->vertex_properties[dst])
          {
            spdlog::error(
                "[Proc {}] Vertex {} and {} have different components: {} and {}",
                this->worker->node_id,
                gid,
                dst,
                this->vertex_properties[gid],
                this->vertex_properties[dst]);
          }
        }
      },
      galois::loopname("Verify Connected Components"),
      galois::no_stats(),
      galois::steal());
}
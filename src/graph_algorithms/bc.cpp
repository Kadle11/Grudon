#include "graph_algorithms/bc.hpp"

template<typename VertexProperty>
BC<VertexProperty>::BC(
    NODE_TYPE node_type,
    std::string_view algorithm_name,
    std::string_view graph_path,
    std::size_t num_compute,
    std::size_t num_memory,
    unsigned int node_id,
    MPICore& net,
    std::string_view partitioning_scheme_file)
    : GraphAlgorithm<VertexProperty>(
          node_type,
          algorithm_name,
          graph_path,
          num_compute,
          num_memory,
          node_id,
          net,
          partitioning_scheme_file)
{
  this->algorithm_name = "BC";
}

template<typename VertexProperty>
void BC<VertexProperty>::init()
{
  // init vertex properties
  if (this->node_type == COMPUTE_NODE)
  {
    for (GNode i{ 0 }; i < this->worker->num_vertices; ++i)
    {
      this->vertex_properties[i] = std::numeric_limits<VertexProperty>::max();
      this->vertex_updates[i] = std::numeric_limits<VertexProperty>::max();
    }
  }
  else if (this->node_type == MEMORY_NODE)
  {
    for (GNode i{ 0 }; i < this->worker->num_vertices; ++i)
    {
      this->vertex_properties[i] = std::numeric_limits<VertexProperty>::max();
      this->vertex_properties[i] = std::numeric_limits<VertexProperty>::max();
    }
  }

  // init source node
  if (this->worker->node_id == 0)
  {
    this->vertex_properties[0] = 0;
    this->vertex_updates[0] = 0;

    this->frontier.set(0);
  }
}

template<typename VertexProperty>
void BC<VertexProperty>::apply_updates()
{
}

template<typename VertexProperty>
void BC<VertexProperty>::gen_updates()
{
  std::vector<GNode> updated_vertices = this->vertex_properties.getUpdatedVertices();

  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        VertexProperty n_dist = this->vertex_properties[lid] + 1;
        auto iiter = this->worker->distributed_graph->lgraph.edge_begin(lid);
        auto eiter = this->worker->distributed_graph->lgraph.edge_end(lid);
        for (; iiter != eiter; ++iiter)
        {
          GNode dst = this->worker->distributed_graph->lgraph.getEdgeDst(iiter);

          this->vertex_updates.minUpdate(dst, n_dist);
        }
      },
      galois::loopname("Generate Updates"),
      galois::no_stats(),
      galois::steal());
}

template<typename VertexProperty>
void BC<VertexProperty>::update_frontier()
{
  galois::substrate::SimpleLock lock;

  std::vector<GNode> updated_vertices = this->vertex_updates.getUpdatedVertices();

  galois::do_all(
      galois::iterate(updated_vertices.begin(), updated_vertices.end()),
      [&](GNode lid)
      {
        VertexProperty update_val = this->vertex_updates[lid];
        if (this->vertex_properties[lid] > update_val)
        {
          this->vertex_properties[lid] = update_val;
          this->frontier.set(lid);
        }
      },
      galois::loopname("Update Frontier"),
      galois::no_stats(),
      galois::steal());
}

template<typename VertexProperty>
void BC<VertexProperty>::aggregate(GNode& lid, const VertexProperty& buffer_val)
{
  this->vertex_updates.minUpdate(lid, buffer_val);
}

template<typename VertexProperty>
bool BC<VertexProperty>::termination_check()
{
  return !static_cast<bool>(this->frontier.count());
}

template<typename VertexProperty>
void BC<VertexProperty>::printState()
{
  if (this->worker->node_type == COMPUTE_NODE)
  {
    for (GNode i{ 0 }; i < this->worker->num_vertices; ++i)
    {
      spdlog::info(
          "[Proc {}] Vertex/Dist: {}/{}",
          this->worker->node_id,
          this->worker->distributed_graph->getGlobalNode(i),
          this->vertex_properties[i]);
    }
  }
}

template<typename VertexProperty>
void BC<VertexProperty>::verify()
{
  if (this->worker->node_type == MEMORY_NODE)
  {
    return;
  }

  Graph vGraph;
  galois::graphs::readGraph(vGraph, this->graph_path);

  galois::GAccumulator<VertexProperty> total_visited_dist;
  galois::GReduceMax<VertexProperty> max_dist;

  galois::do_all(
      galois::iterate(vGraph),
      [&](const GNode& src)
      {
        VertexProperty sd = this->vertex_properties[src];
        if (sd == std::numeric_limits<VertexProperty>::max())
        {
          return;
        }

        for (auto iter : vGraph.edges(src))
        {
          auto dst = vGraph.getEdgeDst(iter);
          VertexProperty dd = this->vertex_properties[dst];

          if (dd > sd + 1)
          {
            spdlog::error("Wrong label: {} on node: {}, correct label from src node {} is {}", dd, dst, src, sd + 1);
          }
        }

        total_visited_dist += 1;
        max_dist.update(sd);
      },
      galois::loopname("Verification"),
      galois::no_stats(),
      galois::steal());

  spdlog::info("Max Dist: {}", max_dist.reduce());
  spdlog::info("Total Visited Dist: {}", total_visited_dist.reduce());
}

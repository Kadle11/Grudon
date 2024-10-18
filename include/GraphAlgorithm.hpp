#ifndef ALGORITHM_HPP
#define ALGORITHM_HPP

#include "DistributedGraph.hpp"
#include "Graph.hpp"
#include "Workers.hpp"

#include <galois/PriorityQueue.h>

#define MAX_ITERATIONS 1000

template<typename VertexProperty>
class Worker;

template<typename VertexProperty>
class UpdateWorker;

template<typename VertexProperty>
class TraverseWorker;

template<typename VertexProperty>
class AggregateWorker;

template<typename VertexProperty>
class GraphAlgorithm
{
 public:
  GraphAlgorithm(
      NODE_TYPE node_type,
      std::string algorithm_name,
      std::string graph_path,
      size_t num_compute,
      size_t num_memory,
      uint32_t node_id,
      MPICore& net);

  virtual void init() = 0;
  virtual void apply_updates() = 0;
  virtual void gen_updates() = 0;
  virtual void update_frontier() = 0;
  virtual void aggregate(GNode& lid, const VertexProperty& buffer_val) = 0;
  virtual bool termination_check() = 0;
  virtual void printState() = 0;

  void run();

 protected:
  std::string algorithm_name;
  galois::ThreadSafeOrderedSet<GNode> frontier;
  PropertyList<VertexProperty> vertex_properties;
  PropertyList<VertexProperty> vertex_updates;
  Worker<VertexProperty>* worker;

  NODE_TYPE node_type;
  MPI_Datatype MPI_VERTEX_PROPERTY_T;
  MPICore& net;

  uint32_t worker_completion_count;
  uint32_t num_workers;
  uint32_t num_compute;
  uint32_t num_memory;

  OFFLOAD_DECISION ndp_decision;
  OFFLOAD_DECISION inc_decision;

  std::vector<galois::LargeArray<VertexProperty>> propertyBuffers;
  galois::DynamicBitSet updatedVertices;
  bool clear_updates;

  // All Workers need access to the Graph Algorithm Interface

  friend class UpdateWorker<VertexProperty>;
  friend class TraverseWorker<VertexProperty>;
  friend class AggregateWorker<VertexProperty>;
  friend class Worker<VertexProperty>;
};

// Explicit Instantiation
template class GraphAlgorithm<float>;
template class GraphAlgorithm<double>;
template class GraphAlgorithm<uint64_t>;
// template class GraphAlgorithm<uint32_t>;


#endif  // ALGORITHM_HPP
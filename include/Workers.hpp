#ifndef WORKERS_HPP
#define WORKERS_HPP

#include "DistributedGraph.hpp"
#include "Graph.hpp"
#include "GraphAlgorithm.hpp"
#include "MPI.hpp"

template<typename T>
class GraphAlgorithm;

template<typename T>
class Worker
{
 public:
  Worker(
      std::string& graph_path,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  ~Worker();

  // FIXME: Avoid the Translation
  uint32_t getVertexComputePartition(const GNode& lid);
  uint32_t getVertexMemoryPartition(const GNode& lid);

  std::vector<galois::DynamicBitSet> bitCommVector;
  std::vector<std::unordered_map<GNode, GNode>> sTranslationTable;
  std::vector<std::unordered_map<GNode, GNode>> rTranslationTable;
  DistributedGraph* distributed_graph;
  uint64_t num_vertices;
  galois::LargeArray<bool> coverage_vector;
  galois::LargeArray<uint64_t> out_degrees;

  MPICore& net;
  size_t num_compute;
  size_t num_memory;

  uint32_t node_id;
  uint64_t total_vertices;
  uint64_t num_edges;
  NODE_TYPE node_type;

  virtual void update(GraphAlgorithm<T>& algorithm) = 0;
  virtual void traverse(GraphAlgorithm<T>& algorithm) = 0;
  virtual void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) = 0;
};

template<typename T>
class UpdateWorker : public Worker<T>
{
 public:
  UpdateWorker(
      std::string& graph_path,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  ~UpdateWorker();

 private:
  void update(GraphAlgorithm<T>& algorithm) override;
  void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) override;

  // Dummy Function
  void traverse(GraphAlgorithm<T>& algorithm) override;
};

template<typename T>
class TraverseWorker : public Worker<T>
{
 public:
  TraverseWorker(
      std::string& graph_path,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  ~TraverseWorker();

 private:
  void traverse(GraphAlgorithm<T>& algorithm);

  // Dummy Functions
  void update(GraphAlgorithm<T>& algorithm) override;
  void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) override;
};

template<typename T>
class AggregateWorker : public Worker<T>
{
 public:
  AggregateWorker(
      std::string& graph_path,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  ~AggregateWorker();

 private:
  void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val);

  // Dummy Functions
  void update(GraphAlgorithm<T>& algorithm) override;
  void traverse(GraphAlgorithm<T>& algorithm) override;
};

// Explicit Instantiation
template class Worker<float>;
template class Worker<double>;
template class Worker<uint64_t>;
template class Worker<uint32_t>;

template class UpdateWorker<float>;
template class UpdateWorker<double>;
template class UpdateWorker<uint64_t>;
template class UpdateWorker<uint32_t>; 

template class TraverseWorker<float>;
template class TraverseWorker<double>;
template class TraverseWorker<uint64_t>;
template class TraverseWorker<uint32_t>;

template class AggregateWorker<float>;
template class AggregateWorker<double>;
template class AggregateWorker<uint64_t>;
template class AggregateWorker<uint32_t>;

#endif  // WORKERS_HPP
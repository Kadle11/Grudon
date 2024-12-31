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
      MPICore& net,
      std::string partitioning_scheme_file);

  Worker(
      DistributedGraph* distributed_graph,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  virtual ~Worker();

  inline uint32_t getVertexComputePartition(const GNode& lid);
  inline uint32_t getVertexMemoryPartition(const GNode& lid);

  std::vector<galois::DynamicBitSet> bitCommVector_Send;
  std::vector<galois::DynamicBitSet> bitCommVector_Recv;
  std::vector<std::vector<GNode>> sTranslationTable;
  std::vector<std::vector<GNode>> rTranslationTable;
  std::vector<std::unordered_map<GNode, GNode>> sAggrTranslationTable;
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

  inline virtual void update(GraphAlgorithm<T>& algorithm) = 0;
  inline virtual void traverse(GraphAlgorithm<T>& algorithm) = 0;
  inline virtual void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) = 0;
};

template<typename T>
class AggregateWorker : public Worker<T>
{
 public:
  AggregateWorker(
      DistributedGraph* distributed_graph,
      size_t& num_compute,
      size_t& num_memory,
      uint32_t& node_id,
      NODE_TYPE node_type,
      MPICore& net);
  ~AggregateWorker();
  std::vector<std::pair<galois::DynamicBitSet, galois::LargeArray<T>>> UpdateWorker_Recv_NDP_Offload();

 private:
  inline void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) override;

  // Dummy Functions
  inline void update(GraphAlgorithm<T>& algorithm) override;
  inline void traverse(GraphAlgorithm<T>& algorithm) override;

  std::vector<MPI_Request> bv_requests;
  std::vector<MPI_Request> data_requests;
  std::vector<MPI_Status> statuses;
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
      MPICore& net,
      std::string& partitioning_scheme_file);
  ~UpdateWorker();

  AggregateWorker<T> aggregator_worker;  // Public object of type AggregateWorker

 private:
  inline void update(GraphAlgorithm<T>& algorithm) override;
  inline void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) override;

  // Dummy Function
  inline void traverse(GraphAlgorithm<T>& algorithm) override;
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
      MPICore& net,
      std::string& partitioning_scheme_file);
  ~TraverseWorker();

 private:
  inline void traverse(GraphAlgorithm<T>& algorithm);

  // Dummy Functions
  inline void update(GraphAlgorithm<T>& algorithm) override;
  inline void aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val) override;
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

#ifndef WORKERS_HPP
#define WORKERS_HPP

#include "DistributedGraph.hpp"
#include "Graph.hpp"
#include "MPI.hpp"

template<typename T>
class Worker
{
 public:
  Worker(std::string& graph_path, uint32_t& num_partitions, uint32_t& node_id, NODE_TYPE& node_type);
  ~Worker();

 private:
  DistributedGraph<uint32_t> distributed_graph;
  MPICore net;

  uint32_t node_id;
  uint64_t num_vertices;
  NODE_TYPE node_type;

  std::vector<galois::DynamicBitSet> bitCommVector;
  std::vector<galois::LargeArray<GNode>> addrTranslationTable;

  PropertyList<T> vertex_properties;
};

template<typename T>
class UpdateWorker : public Worker<T>
{
 public:
  UpdateWorker(std::string& graph_path, uint32_t& num_partitions, uint32_t& node_id, NODE_TYPE& node_type);
  ~UpdateWorker();

 private:

  void update();
};

template<typename T>
class TraverseWorker : public Worker<T>
{
 public:
  TraverseWorker(std::string& graph_path, uint32_t& num_partitions, uint32_t& node_id, NODE_TYPE& node_type);
  ~TraverseWorker();

 private:

  void traverse();
};

template<typename T>
class AggregateWorker : public Worker<T>
{
 public:
  AggregateWorker(std::string& graph_path, uint32_t& num_partitions, uint32_t& node_id, NODE_TYPE& node_type);
  ~AggregateWorker();

 private:

  void aggregate();
};

#endif  // WORKERS_HPP
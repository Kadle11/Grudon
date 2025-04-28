#include "Workers.hpp"

#include "Logger.hpp"

template<typename T>
Worker<T>::Worker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net,
    std::string partitioning_scheme_file)
    : net(net),
      node_id(node_id),
      num_compute(num_compute),
      num_memory(num_memory),
      node_type(node_type)
{
  distributed_graph = new DistributedGraph(
      graph_path,
      num_compute,
      num_memory,
      num_vertices,
      total_vertices,
      num_edges,
      node_id,
      node_type,
      bitCommVector_Send,
      bitCommVector_Recv,
      sTranslationTable,
      rTranslationTable,
      sAggrTranslationTable,
      out_degrees,
      coverage_vector,
      net,
      partitioning_scheme_file);

  // distributed_graph->printGraph();
}

template<typename T>
Worker<T>::Worker(
    DistributedGraph* distributed_graph,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net)
    : net(net),
      node_id(node_id),
      num_compute(num_compute),
      num_memory(num_memory),
      node_type(node_type),
      distributed_graph(distributed_graph)
{
}

template<typename T>
Worker<T>::~Worker()
{
}

template<typename T>
inline uint32_t Worker<T>::getVertexComputePartition(const GNode& lid)
{
  return distributed_graph->getMasterPartition(lid);
}

template<typename T>
inline uint32_t Worker<T>::getVertexMemoryPartition(const GNode& lid)
{
  return distributed_graph->getMirrorPartition(lid);
}

// UpdateWorker Functions
template<typename T>
UpdateWorker<T>::UpdateWorker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net,
    std::string& partitioning_scheme_file)
    : Worker<T>(graph_path, num_compute, num_memory, node_id, node_type, net, partitioning_scheme_file),
      aggregator_worker(this->distributed_graph, num_compute, num_memory, node_id, node_type, net)
{
}

template<typename T>
inline void UpdateWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  algorithm.aggregate(lid, buffer_val);
}

template<typename T>
inline void UpdateWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  algorithm.update_frontier();
  algorithm.apply_updates();
}

template<typename T>
inline void UpdateWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
inline void UpdateWorker<T>::setRbvSize_AggWorker(uint64_t rbvSize)
{
  this->rbvSize = rbvSize;
  this->aggregator_worker.rbvSize = rbvSize;
}

template<typename T>
UpdateWorker<T>::~UpdateWorker()
{
}

// TraverseWorker Functions
template<typename T>
TraverseWorker<T>::TraverseWorker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net,
    std::string& partitioning_scheme_file)
    : Worker<T>(graph_path, num_compute, num_memory, node_id, node_type, net, partitioning_scheme_file)
{
}

template<typename T>
inline void TraverseWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  algorithm.gen_updates();
}

template<typename T>
inline void TraverseWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
inline void TraverseWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  // Do Nothing
}

template<typename T>
TraverseWorker<T>::~TraverseWorker()
{
}

// AggregateWorker Functions
template<typename T>
AggregateWorker<T>::AggregateWorker(
    DistributedGraph* distributed_graph,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net)
    : Worker<T>(distributed_graph, num_compute, num_memory, node_id, node_type, net)
{
  this->num_compute = this->distributed_graph->num_compute;
  this->num_memory = this->distributed_graph->num_memory;
  this->num_vertices = this->distributed_graph->num_vertices;
  this->total_vertices = this->distributed_graph->total_vertices;
  this->num_edges = this->distributed_graph->num_edges;
  this->node_id = this->distributed_graph->node_id;
  this->node_type = this->distributed_graph->node_type;
  this->sTranslationTable = this->distributed_graph->sTranslationTable;
  this->rTranslationTable = this->distributed_graph->rTranslationTable;
  this->sAggrTranslationTable = this->distributed_graph->sAggrTranslationTable;
  this->bitCommVector_Recv.resize(this->num_memory);

  for (size_t i = 0; i < this->num_memory; i++)
  {
    this->bitCommVector_Recv[i].resize(this->num_vertices);
  }

  bv_requests = std::vector<MPI_Request>(num_memory);
  data_requests = std::vector<MPI_Request>(num_memory);
  statuses = std::vector<MPI_Status>(num_memory);
}

template<typename T>
inline void AggregateWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  algorithm.aggregate(lid, buffer_val);
}

template<typename T>
inline void AggregateWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
inline void AggregateWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
AggregateWorker<T>::~AggregateWorker()
{
}

template<typename T>
std::vector<std::pair<galois::DynamicBitSet, galois::LargeArray<T>>> AggregateWorker<T>::UpdateWorker_Recv_NDP_Offload()
{
  galois::DynamicBitSet aggregateBitCommVector;
  galois::LargeArray<T> aggregatePropertyBuffer;
  std::vector<galois::LargeArray<T>> propertyBuffers(this->num_memory);
  std::vector<std::pair<galois::DynamicBitSet, galois::LargeArray<T>>> result;
  aggregateBitCommVector.resize(this->num_vertices);
  aggregatePropertyBuffer.allocateLocal(this->num_vertices);
  
  auto MPI_VERTEX_PROPERTY_T = mpi_get_type<T>();

  std::map<GNode, T> aggregatePropertyMap;

  uint64_t bytes_recv = 0;
  uint64_t bytes_sent = 0;
  uint64_t in_bytes = 0;
  uint64_t out_bytes = 0;
  uint64_t agg_ops = 0;
  uint64_t no_agg_bytes = this->rbvSize;
  uint64_t elem_bytes = 0;

  for (int i = 0; i < this->num_memory; i++)
  {
    propertyBuffers[i].allocateLocal(this->sTranslationTable[i].size());

    bytes_recv = this->net.Irecv(
        i + this->num_compute,
        0,
        this->bitCommVector_Recv[i].bitvec.data(),
        this->bitCommVector_Recv[i].size_bytes(),
        MPI_UINT64_T,
        &this->statuses[i],
        &this->bv_requests[i]);

    elem_bytes = this->net.Irecv(
        i + this->num_compute,
        0,
        propertyBuffers[i].data(),
        propertyBuffers[i].size(),
        MPI_VERTEX_PROPERTY_T,
        &this->statuses[i],
        &this->data_requests[i]);

    in_bytes += aggregateBitCommVector.size_bytes() * sizeof(uint64_t) + elem_bytes;
    no_agg_bytes += elem_bytes;

    this->net.decrementBytesMoved(bytes_recv);
    this->net.incrementBytesMoved(aggregateBitCommVector.size_bytes() * sizeof(uint64_t));

    size_t bitCommVectorSize = this->bitCommVector_Recv[i].size();
    uint32_t local_idx = 0;
 
    for (size_t j = 0; j < bitCommVectorSize; ++j)
    {
      if (this->bitCommVector_Recv[i].test(j))
      {
        if (!aggregateBitCommVector.test(j))
        {
          aggregateBitCommVector.set(j);
          aggregatePropertyMap[j] = propertyBuffers[i][local_idx];
        }
        else
        {
          aggregatePropertyMap[j] += propertyBuffers[i][local_idx];
          // aggregatePropertyMap[j] = std::min(aggregatePropertyMap[j], propertyBuffers[i][local_idx]);

          agg_ops++;
        }
        local_idx++;
      }
    }

    this->bitCommVector_Recv[i].reset();
  }

  uint32_t idx = 0;
  for (const auto& elem : aggregatePropertyMap)
  {
    aggregatePropertyBuffer[idx] = elem.second;
    idx++;
  }

  bytes_sent += idx * sizeof(T);
  bytes_sent += aggregateBitCommVector.size_bytes() * sizeof(uint64_t);
  out_bytes = bytes_sent;

  this->net.incrementBytesMoved(bytes_sent); // M2S and S2C Tracked!

  result.emplace_back(std::move(aggregateBitCommVector), std::move(aggregatePropertyBuffer));

  spdlog::info(
      "[Proc {}] Agg Reduction: {}/{}, Ops: {}, NoAggBytes: {}, RBV Size: {}",
      this->node_id,
      in_bytes,
      out_bytes,
      agg_ops,
      no_agg_bytes * 2,
      this->rbvSize
  );

  return result;
}

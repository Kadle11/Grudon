#include "Workers.hpp"

#include "Logger.hpp"

template<typename T>
Worker<T>::Worker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net)
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
      bitCommVector,
      sTranslationTable,
      rTranslationTable,
      out_degrees,
      coverage_vector,
      net);
  // distributed_graph->printGraph();
}

template<typename T>
Worker<T>::Worker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net,
    DistributedGraph* graph)
    : net(net),
      node_id(node_id),
      num_compute(num_compute),
      num_memory(num_memory),
      node_type(node_type),
      distributed_graph(graph)
{
  num_vertices = distributed_graph->num_vertices;
  total_vertices = distributed_graph->total_vertices;
  num_edges = distributed_graph->num_edges;
  sTranslationTable = distributed_graph->sTranslationTable;
  rTranslationTable = distributed_graph->rTranslationTable;
  if (node_type == MEMORY_NODE)
  {
    bitCommVector.resize(num_compute);
    for (int i = 0; i < num_compute; i++)
    {
      bitCommVector[i].resize(this->sTranslationTable[i].size());
    }
  }
  else if (node_type == COMPUTE_NODE)
  {
    bitCommVector.resize(num_memory);
    for (int i = 0; i < num_memory; i++)
    {
      bitCommVector[i].resize(this->sTranslationTable[i].size());
    }
  }
  // distributed_graph->printGraph();
}

template<typename T>
Worker<T>::~Worker()
{
}

template<typename T>
uint32_t Worker<T>::getVertexComputePartition(const GNode& lid)
{
  return distributed_graph->getMasterPartition(lid);
}

template<typename T>
uint32_t Worker<T>::getVertexMemoryPartition(const GNode& lid)
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
    MPICore& net)
    : Worker<T>(graph_path, num_compute, num_memory, node_id, node_type, net),
      aggregator(graph_path, num_compute, num_memory, node_id, node_type, net, this->distributed_graph)
{
}

template<typename T>
void UpdateWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  algorithm.aggregate(lid, buffer_val);
}

template<typename T>
void UpdateWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  algorithm.update_frontier();
  algorithm.apply_updates();
}

template<typename T>
void UpdateWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
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
    MPICore& net)
    : Worker<T>(graph_path, num_compute, num_memory, node_id, node_type, net)
{
}

template<typename T>
void TraverseWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  algorithm.gen_updates();
}

template<typename T>
void TraverseWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
void TraverseWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  // Do Nothing
}

template<typename T>
TraverseWorker<T>::~TraverseWorker()
{
}

// AggregateSwitch Functions
template<typename T>
AggregateWorker<T>::AggregateWorker(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint32_t& node_id,
    NODE_TYPE node_type,
    MPICore& net,
    DistributedGraph* graph)
    : Worker<T>(graph_path, num_compute, num_memory, node_id, node_type, net, graph)
{
  propertyBuffers.resize(num_memory);
  for (int i = 0; i < num_memory; ++i)
  {
    propertyBuffers[i].allocateLocal(this->sTranslationTable[i].size());
  }
}

template<typename T>
void AggregateWorker<T>::aggregate(GraphAlgorithm<T>& algorithm, GNode& lid, const T& buffer_val)
{
  algorithm.aggregate(lid, buffer_val);
}

template<typename T>
void AggregateWorker<T>::update(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
void AggregateWorker<T>::traverse(GraphAlgorithm<T>& algorithm)
{
  // Do Nothing
}

template<typename T>
AggregateWorker<T>::~AggregateWorker()
{
}

template<typename T>
std::vector<std::pair<galois::DynamicBitSet, galois::LargeArray<T>>> AggregateWorker<T>::aggregate_chunkwise(
    GraphAlgorithm<T>& algorithm,
    const size_t& chunk_size)
{
  MPI_Datatype MPI_VERTEX_PROPERTY_T = mpi_get_type<T>();
  std::vector<std::pair<galois::DynamicBitSet, galois::LargeArray<T>>> return_vector;
  size_t bitCommVectorSize = this->sTranslationTable.size();
  std::cout << "Loc1" << std::endl;
  for (size_t start_memory_node = 0; start_memory_node < this->num_memory; start_memory_node += chunk_size)
  {
    galois::DynamicBitSet chunk_bitCommVector;
    std::unordered_map<size_t, T> chunk_update_map;
    galois::LargeArray<T> propertyBuffer;
    chunk_bitCommVector.resize(bitCommVectorSize);
    std::cout << "Loc2" << std::endl;
    for (size_t i = start_memory_node; i < start_memory_node + chunk_size && i < this->num_memory; ++i)
    {
      std::cout << "Loc3" << std::endl;
      galois::DynamicBitSet recv_bitCommVector;
      galois::LargeArray<T> recv_propertyBuffer;
      recv_bitCommVector.resize(bitCommVectorSize);
      recv_propertyBuffer.allocateLocal(bitCommVectorSize);

      this->net.recv(
          i + this->num_compute,
          0,
          recv_bitCommVector.bitvec.data(),
          recv_bitCommVector.size_bytes(),
          MPI_UINT64_T,
          MPI_STATUS_IGNORE);

      this->net.recv(
          i + this->num_compute,
          0,
          recv_propertyBuffer.data(),
          recv_propertyBuffer.size() * sizeof(T),  // Ensure correct size in bytes
          MPI_VERTEX_PROPERTY_T,
          MPI_STATUS_IGNORE);

      size_t idx = 0;
      for (size_t j = 0; j < bitCommVectorSize; ++j)
      {
        if (recv_bitCommVector.test(j))
        {
          if (chunk_bitCommVector.test(j))
          {
            chunk_update_map[j] = algorithm.aggregate_value(chunk_update_map[j], recv_propertyBuffer[idx++]);
          }
          else
          {
            chunk_bitCommVector.set(j);
            chunk_update_map[j] = recv_propertyBuffer[idx++];
          }
        }
        std::cout << "Loc4" << std::endl;
      }
    }
    std::cout << "Loc5" << std::endl;
    size_t idx=0;
    propertyBuffer.allocateLocal(chunk_bitCommVector.count());
    for (size_t j = 0; j < bitCommVectorSize; ++j)
    {
      if (chunk_bitCommVector.test(j))
      {
        propertyBuffer[idx++] = chunk_update_map[j];
      }
    }
    std::cout << "Loc6" << std::endl;
    return_vector.push_back(std::make_pair(std::move(chunk_bitCommVector), std::move(propertyBuffer)));
    std::cout << "Loc7" << std::endl;
  }
  return return_vector;
}
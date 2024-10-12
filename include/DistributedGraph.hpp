#ifndef DISTRIBUTEDGRAPH_HPP
#define DISTRIBUTEDGRAPH_HPP

/**
 * @file DistributedGraph.hpp
 * @brief This file contains the definition of the DistributedGraph class.
 *
 * Flow of Control:
 * 1. Read Graph on Proc 0
 * 2. Partition Graph using chosen partitioning scheme
 * 3. Distribute Graph to all procs
 * 4. Create PropertyList for vertices in partitions
 * 5. Create the structure for Memoization for Address Translation
 * 6. Create the BitVector for Communication
 * 7. Conditional Message Passing depending on Sparse/Dense Communication
 */

#include <galois/PriorityQueue.h>
#include <galois/substrate/SimpleLock.h>

#include <boost/iterator/counting_iterator.hpp>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "Graph.hpp"
#include "Logger.hpp"
#include "MPI.hpp"

using Graph = galois::graphs::LC_CSR_Graph<uint64_t, uint32_t>::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

template<typename T>
struct dataElement : public galois::runtime::Lockable
{
 public:
  using reference = T&;

  T v;

  reference getData()
  {
    return v;
  }
};

template<typename T>
struct PropertyList
{
 public:
  PropertyList() = default;

  typename dataElement<T>::reference operator[](const GNode& n)
  {
    std::unique_lock<std::shared_mutex> lock(locks[n]);
    return data[n].getData();
  }

  typename dataElement<T>::reference getData(const GNode& n, galois::MethodFlag mflag = galois::MethodFlag::READ)
  {
    std::shared_lock<std::shared_mutex> lock(locks[n]);
    return data[n].getData();
  }

  void minUpdate(const GNode& n, const T& val)
  {
    updated_vertices.insert(n);

    std::unique_lock<std::shared_mutex> lock(locks[n]);
    data[n].v = std::min(data[n].v, val);
  }

  void maxUpdate(const GNode& n, const T& val)
  {
    updated_vertices.insert(n);

    std::unique_lock<std::shared_mutex> lock(locks[n]);
    data[n].v = std::max(data[n].v, val);
  }

  void addUpdate(const GNode& n, const T& val)
  {
    updated_vertices.insert(n);

    std::unique_lock<std::shared_mutex> lock(locks[n]);
    data[n].v += val;
  }

  void set(const GNode& n, const T& val)
  {
    updated_vertices.insert(n);

    std::unique_lock<std::shared_mutex> lock(locks[n]);
    data[n].v = val;
  }

  void clear()
  {
    galois::do_all(
        galois::iterate(updated_vertices.begin(), updated_vertices.end()),
        [&](const GNode& n)
        {
          std::unique_lock<std::shared_mutex> lock(locks[n]);
          data[n].v = 0;
        },
        galois::loopname("Clear Updated Vertices"),
        galois::no_stats(),
        galois::steal());

    updated_vertices.clear();
  }

  void clear_uv()
  {
    updated_vertices.clear();
  }

  size_t uv_size()
  {
    return updated_vertices.size();
  }

  void allocate(size_t size)
  {
    data.allocateInterleaved(size);
    locks = std::vector<std::shared_mutex>(size);

    updated_vertices = galois::ThreadSafeOrderedSet<GNode>();
  }

  void addUpdatedVertex(const GNode& n)
  {
    updated_vertices.insert(n);
  }

  using iterator = boost::counting_iterator<GNode>;
  iterator begin()
  {
    return iterator(0);
  }
  iterator end()
  {
    return iterator(data.size());
  }

  size_t size()
  {
    return data.size();
  }

  galois::ThreadSafeOrderedSet<GNode>& getUpdatedVertices()
  {
    return updated_vertices;
  }

  galois::LargeArray<dataElement<T>> data;
  galois::ThreadSafeOrderedSet<GNode> updated_vertices;
  std::vector<std::shared_mutex> locks;
};

// Explicit Instantiation
template class PropertyList<float>;
template class PropertyList<double>;
template class PropertyList<uint64_t>;

class DistributedGraph
{
 public:
  DistributedGraph(
      std::string& graph_path,
      size_t& num_compute,
      size_t& num_memory,
      uint64_t& num_vertices,
      uint64_t& total_vertices,
      uint64_t& num_edges,
      uint32_t& node_id,
      NODE_TYPE& node_type,
      std::vector<galois::DynamicBitSet>& bitCommVector,
      std::vector<std::unordered_map<GNode, GNode>>& sTranslationTable,
      std::vector<std::unordered_map<GNode, GNode>>& rTranslationTable,
      galois::LargeArray<uint64_t>& out_degrees,
      galois::LargeArray<bool>& coverage_vector,
      MPICore& net);
  ~DistributedGraph();

  bool isCoverageComplete(std::vector<GNode>& frontier);

  GNode getLocalNode(const GNode& gid);
  GNode getGlobalNode(const GNode& lid);
  uint64_t getOutDegree(const GNode& lid);

  uint64_t getMirrorPartition(const GNode& gid);
  uint64_t getMasterPartition(const GNode& gid);

  void printState();
  void printGraph();

  uint64_t getNumVertices()
  {
    return num_vertices;
  }

  Graph lgraph;

 private:
  size_t& num_compute;
  size_t& num_memory;
  uint64_t& num_vertices;
  uint64_t& total_vertices;
  uint64_t& num_edges;
  uint32_t& node_id;
  NODE_TYPE& node_type;

  // TODO: Make sure these are structures only required for initialization
  galois::LargeArray<GNode> mirror_partition;
  galois::LargeArray<GNode> master_partition;
  std::vector<uint64_t> master_partition_sizes;
  std::vector<uint64_t> mirror_partition_sizes;
  Graph bgraph;

  std::vector<galois::DynamicBitSet>& bitCommVector;
  std::vector<std::unordered_map<GNode, GNode>>& sTranslationTable;
  std::vector<std::unordered_map<GNode, GNode>>& rTranslationTable;
  MPICore& net;

  std::unordered_map<GNode, GNode> gid_to_lid;
  std::unordered_map<GNode, GNode> lid_to_gid;
  galois::LargeArray<bool>& coverage_vector;
  galois::LargeArray<uint64_t>& out_degrees;
};

#endif  // DISTRIBUTEDGRAPH_HPP
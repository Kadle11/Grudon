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

#include <galois/substrate/SimpleLock.h>

#include <boost/iterator/counting_iterator.hpp>
#include <set>
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

struct ThreadSafeSet
{
  std::set<GNode> set;
  std::mutex mtx;

  const std::set<GNode>& get()
  {
    return set;
  }

  void clear()
  {
    std::lock_guard<std::mutex> lock(mtx);
    set.clear();
  }

  void insert(const GNode& n)
  {
    std::lock_guard<std::mutex> lock(mtx);
    set.insert(n);
  }

  size_t size()
  {
    return set.size();
  }
};

template<typename T>
struct PropertyList
{
 public:
  PropertyList() = default;

  typename dataElement<T>::reference operator[](const GNode& n)
  {
    acquireNode(n, galois::MethodFlag::WRITE);
    return data[n].getData();
  }

  typename dataElement<T>::reference getData(const GNode& n, galois::MethodFlag mflag = galois::MethodFlag::READ)
  {
    acquireNode(n, mflag);
    return data[n].getData();
  }

  void minUpdate(const GNode& n, const T& val)
  {
    acquireNode(n);
    data[n].v = std::min(data[n].v, val);
    updated_vertices.insert(n);
  }

  void maxUpdate(const GNode& n, const T& val)
  {
    acquireNode(n);
    data[n].v = std::max(data[n].v, val);
    updated_vertices.insert(n);
  }

  void addUpdate(const GNode& n, const T& val)
  {
    acquireNode(n);
    data[n].v += val;
    updated_vertices.insert(n);
  }

  void set(const GNode& n, const T& val)
  {
    acquireNode(n);
    data[n].v = val;
    updated_vertices.insert(n);
  }

  void clear()
  {
    galois::do_all(
        galois::iterate(updated_vertices.set.begin(), updated_vertices.set.end()),
        [&](const GNode& n)
        {
          acquireNode(n);
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

  const std::set<GNode>& getUpdatedVertices()
  {
    return updated_vertices.get();
  }

  void acquireNode(const GNode& node, galois::MethodFlag mflag = galois::MethodFlag::WRITE)
  {
    assert(node < data.size());
    galois::runtime::acquire(&data[node], mflag);
  }

  galois::LargeArray<dataElement<T>> data;
  ThreadSafeSet updated_vertices;
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
      std::vector<galois::LargeArray<GNode>>& addrTranslationTable,
      galois::LargeArray<uint64_t>& out_degrees,
      galois::LargeArray<bool>& coverage_vector,
      MPICore& net);
  ~DistributedGraph();

  bool isCoverageComplete(std::vector<GNode>& frontier);

  GNode getLocalNode(GNode& gid);
  GNode getGlobalNode(GNode& lid);
  uint64_t getOutDegree(GNode& lid);

  uint64_t getMirrorPartition(const GNode& gid);
  uint64_t getMasterPartition(const GNode& gid);

  void printState();

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
  std::vector<galois::LargeArray<GNode>>& addrTranslationTable;

  MPICore& net;

  std::unordered_map<GNode, GNode> gid_to_lid;
  std::unordered_map<GNode, GNode> lid_to_gid;
  galois::LargeArray<bool>& coverage_vector;
  galois::LargeArray<uint64_t>& out_degrees;
};

#endif  // DISTRIBUTEDGRAPH_HPP
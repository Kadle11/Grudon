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

#include <galois/AtomicHelpers.h>
#include <galois/DynamicBitset.h>
#include <galois/PriorityQueue.h>
#include <galois/substrate/SimpleLock.h>
#include <spdlog/fmt/ostr.h>

#include <atomic>
#include <boost/iterator/counting_iterator.hpp>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "Graph.hpp"
#include "Logger.hpp"
#include "MPI.hpp"


using Graph = galois::graphs::LC_CSR_Graph<uint64_t, uint32_t>::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

// Create a AtomicElement Class
template<typename T>
class AtomicElement : public std::atomic<T>
{
 public:
  AtomicElement() = default;
  AtomicElement(const T& val) : std::atomic<T>(val)
  {
  }

  AtomicElement& operator=(const T& val)
  {
    std::atomic<T>::store(val, std::memory_order_relaxed);
    return *this;
  }

  operator T() const
  {
    return std::atomic<T>::load(std::memory_order_relaxed);
  }

  AtomicElement& operator=(const AtomicElement& other)
  {
    std::atomic<T>::store(other.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }
};

// Formatter for AtomicElement
template<typename T>
struct fmt::formatter<AtomicElement<T>>
{
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(const AtomicElement<T>& p, FormatContext& ctx)
  {
    return format_to(ctx.out(), "{}", static_cast<T>(p));
  }
};

// Explicit Instantiation
template class AtomicElement<float>;
template class AtomicElement<double>;
template class AtomicElement<uint64_t>;
template class AtomicElement<uint32_t>;

template<typename T>
struct PropertyList : galois::LargeArray<AtomicElement<T>>
{
 public:
  // T operator[](const GNode& n) const
  // {
  //   return data[n];
  // }

  // T operator[](const GNode& n)
  // {
  //   return data[n];
  // }
  void minUpdate(const GNode& n, const T& val)
  {
    const T& prev_val = galois::atomicMin(galois::LargeArray<AtomicElement<T>>::operator[](n), val);

    if (val < prev_val)
    {
      // updated_vertices.insert(n);
      updated_vertices_bitset.set(n);
    }
  }

  void maxUpdate(const GNode& n, const T& val)
  {
    const T& prev_val = galois::atomicMax(galois::LargeArray<AtomicElement<T>>::operator[](n), val);

    if (val > prev_val)
    {
      // updated_vertices.insert(n);
      updated_vertices_bitset.set(n);
    }
  }

  void addUpdate(const GNode& n, const T& val)
  {
    // updated_vertices.insert(n);
    updated_vertices_bitset.set(n);
    galois::atomicAdd(galois::LargeArray<AtomicElement<T>>::operator[](n), val);
  }

  void set(const GNode& n, const T& val)
  {
    // updated_vertices.insert(n);
    updated_vertices_bitset.set(n);
    AtomicElement<T>& element = galois::LargeArray<AtomicElement<T>>::operator[](n);
    element.store(val, std::memory_order_relaxed);
  }

  void clear()
  {
    auto bitvector = updated_vertices_bitset.getOffsets();
    // galois::do_all(
    //     galois::iterate(updated_vertices.begin(), updated_vertices.end()),
    //     [&](const GNode& n)
    //     {
    //       std::unique_lock<std::shared_mutex> lock(locks[n]);
    //       data[n].v = 0;
    //     },
    //     galois::loopname("Clear Updated Vertices"),
    //     galois::no_stats(),
    //     galois::steal());
    galois::do_all(
        galois::iterate(bitvector.begin(), bitvector.end()),
        [&](const GNode& n)
        {
          // std::unique_lock<std::shared_mutex> lock(locks[n]);
          galois::LargeArray<AtomicElement<T>>::operator[](n) = 0;
        },
        galois::loopname("Clear Updated Vertices"),
        galois::no_stats(),
        galois::steal());

    // updated_vertices.clear();
    updated_vertices_bitset.reset();
  }

  void clear_uv()
  {
    // updated_vertices.clear();
    updated_vertices_bitset.reset();
  }

  size_t uv_size()
  {
    // return updated_vertices.size();
    return updated_vertices_bitset.count();
  }

  void allocate(size_t size)
  {
    // updated_vertices = galois::ThreadSafeOrderedSet<GNode>();
    galois::LargeArray<AtomicElement<T>>::allocateLocal(size);
    updated_vertices_bitset.resize(size);
  }

  void addUpdatedVertex(const GNode& n)
  {
    // updated_vertices.insert(n);
    updated_vertices_bitset.set(n);
  }

  using iterator = boost::counting_iterator<GNode>;
  iterator begin()
  {
    return iterator(0);
  }
  iterator end()
  {
    return iterator(galois::LargeArray<AtomicElement<T>>::size());
  }

  size_t size()
  {
    return galois::LargeArray<AtomicElement<T>>::size();
  }

  std::vector<GNode> getUpdatedVertices()
  {
    return updated_vertices_bitset.getOffsets();
  }

  galois::DynamicBitSet updated_vertices_bitset;
};

// Explicit Instantiation
template class PropertyList<float>;
template class PropertyList<double>;
template class PropertyList<uint64_t>;
template class PropertyList<uint32_t>;

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
      std::vector<galois::DynamicBitSet>& bitCommVector_Send,
      std::vector<galois::DynamicBitSet>& bitCommVector_Recv,
      std::vector<std::vector<GNode>>& sTranslationTable,
      std::vector<std::vector<GNode>>& rTranslationTable,
      std::vector<std::unordered_map<GNode, GNode>>& sAggrTranslationTable,
      galois::LargeArray<uint64_t>& out_degrees,
      galois::LargeArray<bool>& coverage_vector,
      MPICore& net,
      std::string& partitioning_scheme_file);
  ~DistributedGraph();

  bool isCoverageComplete(std::vector<GNode>& frontier);

  GNode getLocalNode(const GNode& gid);
  GNode getGlobalNode(const GNode& lid);
  uint64_t getOutDegree(const GNode& lid);

  uint64_t getMirrorPartition(const GNode& lid);
  uint64_t getMasterPartition(const GNode& lid);

  void printState();
  void printGraph();

  uint64_t getNumVertices()
  {
    return num_vertices;
  }

  Graph lgraph;

 
  size_t& num_compute;
  size_t& num_memory;
  uint64_t& num_vertices;
  uint64_t& total_vertices;
  uint64_t& num_edges;
  uint32_t& node_id;
  NODE_TYPE& node_type;
  double replication_factor;
  double avg_degree;

  // TODO: Make sure these are structures only required for initialization
  galois::LargeArray<GNode> mirror_partition;
  galois::LargeArray<GNode> master_partition;
  std::vector<uint64_t> master_partition_sizes;
  std::vector<uint64_t> mirror_partition_sizes;
  Graph bgraph;

  std::vector<galois::DynamicBitSet>& bitCommVector_Send;
  std::vector<galois::DynamicBitSet>& bitCommVector_Recv;
  std::vector<std::vector<GNode>>& sTranslationTable;
  std::vector<std::vector<GNode>>& rTranslationTable;
  std::vector<std::unordered_map<GNode, GNode>>& sAggrTranslationTable;
  MPICore& net;

  std::vector<GNode> gid_to_lid;
  std::vector<GNode> lid_to_gid;
  galois::LargeArray<bool>& coverage_vector;
  galois::LargeArray<uint64_t>& out_degrees;
};


double calculateSkew(std::vector<GNode>& frontier, uint32_t& num_memory, DistributedGraph& distributed_graph);

#endif
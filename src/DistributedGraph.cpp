
#include "DistributedGraph.hpp"

#include "Logger.hpp"
#include "partitioning_engine/Partitioner.hpp"

struct vertexEdgeCount
{
  uint64_t vertex;
  uint64_t edge_count;
};

DistributedGraph::DistributedGraph(
    std::string& graph_path,
    size_t& num_compute,
    size_t& num_memory,
    uint64_t& num_vertices,
    uint32_t& node_id,
    NODE_TYPE& node_type,
    std::vector<galois::DynamicBitSet>& bitCommVector,
    std::vector<galois::LargeArray<GNode>>& addrTranslationTable,
    MPICore& net)
    : num_compute(num_compute),
      num_memory(num_memory),
      node_id(node_id),
      node_type(node_type),
      bitCommVector(bitCommVector),
      addrTranslationTable(addrTranslationTable),
      num_vertices(num_vertices),
      net(net)
{
  // spdlog::set_level(spdlog::level::debug);

  std::vector<uint64_t> mirror_edge_counts;
  if (node_id == 0)
  {
    // Read the Graph
    galois::graphs::readGraph(bgraph, graph_path);

    spdlog::info("Read {} nodes and {} edges", bgraph.size(), bgraph.sizeEdges());

    // Partition the Graph
    master_partition_sizes.resize(num_compute, 0);
    mirror_partition_sizes.resize(num_memory, 0);

    mirror_edge_counts.resize(num_memory, 0);

    NaivePartitioner(
        bgraph,
        num_compute,
        num_memory,
        mirror_partition,
        master_partition,
        master_partition_sizes,
        mirror_partition_sizes,
        mirror_edge_counts);

    spdlog::info("Partitioned the across {} masters and {} mirrors", num_compute, num_memory);
  }

  // Distribute the number of vertices to all nodes
  uint64_t num_edges = 0;
  uint64_t total_vertices = 0;
  if (node_id == 0)
  {
    total_vertices = bgraph.size();
    num_vertices = master_partition_sizes[0];
    for (int i = 1; i < num_compute; i++)
    {
      net.send(i, 0, &master_partition_sizes[i], 1, MPI_UINT64_T);
      net.send(i, 0, &total_vertices, 1, MPI_UINT64_T);
      net.send(i, 0, mirror_partition.data(), total_vertices, MPI_GNODE_T);
      net.send(i, 0, mirror_partition_sizes.data(), num_memory, MPI_UINT64_T);
    }

    for (int i = num_compute; i < num_compute + num_memory; i++)
    {
      net.send(i, 0, &mirror_partition_sizes[i - num_compute], 1, MPI_UINT64_T);
      net.send(i, 0, &total_vertices, 1, MPI_UINT64_T);
      net.send(i, 0, master_partition.data(), total_vertices, MPI_GNODE_T);
      net.send(i, 0, master_partition_sizes.data(), num_compute, MPI_UINT64_T);
      net.send(i, 0, &mirror_edge_counts[i - num_compute], 1, MPI_UINT64_T);
    }
  }
  else
  {
    net.recv(0, 0, &num_vertices, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);
    net.recv(0, 0, &total_vertices, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);

    if (node_type == COMPUTE_NODE)
    {
      mirror_partition.allocateInterleaved(total_vertices);
      mirror_partition_sizes.resize(num_memory);

      net.recv(0, 0, mirror_partition.data(), total_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, mirror_partition_sizes.data(), num_memory, MPI_UINT64_T, MPI_STATUS_IGNORE);
    }
    else if (node_type == MEMORY_NODE)
    {
      master_partition.allocateInterleaved(total_vertices);
      master_partition_sizes.resize(num_compute);

      net.recv(0, 0, master_partition.data(), total_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, master_partition_sizes.data(), num_compute, MPI_UINT64_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, &num_edges, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);
    }
  }

  spdlog::info("[Proc {}] Allocating the graph with {} vertices and {} edges", node_id, num_vertices, num_edges);

  net.barrier();
  lgraph.allocateFrom(num_vertices, num_edges);
  lgraph.constructNodes();

  // MPI data type for vertexEdgeCount
  MPI_Datatype vertexEdgeCountType;

  // FIXME: Memory Leak
  MPI_Type_create_struct(
      2,                       // number of blocks
      new int[]{ 1, 1 },       // block lengths
      new MPI_Aint[]{ 0, 8 },  // block offsets
      new MPI_Datatype[]{ MPI_UINT64_T, MPI_UINT64_T },
      &vertexEdgeCountType);

  MPI_Type_commit(&vertexEdgeCountType);

  // Distribute the Vertices/Edges
  // TODO: Chunking for Large Graphs

  if (node_id == 0)
  {
    galois::substrate::SimpleLock lock;
    std::vector<std::map<GNode, GNode>> gid_to_lids(num_compute);
    galois::do_all(
        galois::iterate(size_t(0), num_compute),
        [&](size_t i)
        {
          std::vector<GNode> vbuffer;
          vbuffer.reserve(master_partition_sizes[i]);
          if (i == 0)
          {
            uint64_t vcount = 0;
            for (GNode n = 0; n < master_partition.size(); ++n)
            {
              if (n % num_compute == i)
              {
                lid_to_gid[vcount] = n;
                gid_to_lid[n] = vcount++;
              }
            }
          }
          else
          {
            for (GNode n = 0; n < master_partition.size(); ++n)
            {
              if (n % num_compute == i)
              {
                gid_to_lids[i][n] = vbuffer.size();
                vbuffer.push_back(n);
              }
            }

            spdlog::debug(
                "[Proc {}] Sending vertices {}/{} to compute node {}",
                node_id,
                vbuffer.size(),
                master_partition_sizes[i],
                i);

            assert(vbuffer.size() == master_partition_sizes[i]);

            lock.lock();
            net.send(i, 0, vbuffer.data(), vbuffer.size(), MPI_GNODE_T);
            lock.unlock();

            vbuffer.clear();
          }
        },
        galois::loopname("Distribute Vertices to Masters"));
    spdlog::info("[Proc {}] Distributed the vertices to all compute nodes", node_id);

    galois::do_all(
        galois::iterate(size_t(0), num_memory),
        [&](size_t i)
        {
          std::vector<GNode> vbuffer;
          std::vector<uint64_t> edge_ends_buffer;

          vbuffer.reserve(mirror_partition_sizes[i]);
          edge_ends_buffer.reserve(mirror_partition_sizes[i]);

          for (GNode n = 0; n < mirror_partition.size(); ++n)
          {
            if (mirror_partition[n] == i)
            {
              vbuffer.push_back(n);
              edge_ends_buffer.push_back(std::distance(bgraph.edge_begin(n), bgraph.edge_end(n)));
            }
          }

          assert(vbuffer.size() == mirror_partition_sizes[i]);
          assert(edge_ends_buffer.size() == mirror_partition_sizes[i]);

          lock.lock();
          net.send(i + num_compute, 0, vbuffer.data(), mirror_partition_sizes[i], MPI_GNODE_T);
          net.send(i + num_compute, 0, edge_ends_buffer.data(), mirror_partition_sizes[i], MPI_UINT64_T);
          lock.unlock();

          vbuffer.clear();
          edge_ends_buffer.clear();
        });

    spdlog::info("[Proc {}] Distributed the vertices to all memory nodes", node_id);

    /*
      galois::do_all(
          galois::iterate(bgraph),
          [&](GNode n)
          {
            MPI_Request vec_mpi_buffer = MPI_REQUEST_NULL;
            MPI_Request edge_mpi_buffer = MPI_REQUEST_NULL;

            std::vector<GNode> ebuffer;
            vertexEdgeCount vecSend;
            ebuffer.reserve(std::distance(bgraph.edge_begin(n), bgraph.edge_end(n)));
            for (auto ii = bgraph.edge_begin(n), ei = bgraph.edge_end(n); ii != ei; ++ii)
            {
              ebuffer.push_back(bgraph.getEdgeDst(ii));
            }
            vecSend.vertex = n;
            vecSend.edge_count = ebuffer.size();

            spdlog::debug(
                "[Proc {}] Sending vertex {} with {}/{} edges to memory node {}",
                node_id,
                vecSend.vertex,
                vecSend.edge_count,
                std::distance(bgraph.edge_begin(n), bgraph.edge_end(n)),
                mirror_partition[n] + num_compute);

            assert(vecSend.edge_count == std::distance(bgraph.edge_begin(n), bgraph.edge_end(n)));

            lock.lock();
            net.send(mirror_partition[n] + num_compute, 0, &vecSend, 1, vertexEdgeCountType);
            net.send(mirror_partition[n] + num_compute, 0, ebuffer.data(), ebuffer.size(), MPI_GNODE_T);
            lock.unlock();

            ebuffer.clear();
          },
          galois::chunk_size<1024>(),
          galois::loopname("Distribute Vertices to Mirrors"));
      spdlog::info("[Proc {}] Distributed the edges to all memory nodes", node_id);
    */

    // Create Coverage Vectors and OutDegree Vectors
    std::vector<galois::LargeArray<bool>> coverage_vectors(num_compute);
    std::vector<galois::LargeArray<uint64_t>> out_degree_vectors(num_compute);
    for (int i = 0; i < num_compute; i++)
    {
      coverage_vectors[i].allocateLocal(master_partition_sizes[i]);
      out_degree_vectors[i].allocateLocal(master_partition_sizes[i]);
    }

    out_degrees.allocateLocal(num_vertices);
    coverage_vector.allocateLocal(num_vertices);

    // Initialize the Coverage Vector and Out Degree Vector
    galois::do_all(
        galois::iterate(uint64_t(0), num_vertices),
        [&](uint64_t n)
        {
          coverage_vector[n] = true;
          out_degrees[n] = 0;

          for (int i = 1; i < num_compute; i++)
          {
            coverage_vectors[i][n] = true;
            out_degree_vectors[i][n] = 0;
          }
        },
        galois::loopname("Initialize Coverage Vector and Out Degree Vector"));

    galois::substrate::SimpleLock local_lock;
    galois::do_all(
        galois::iterate(bgraph),
        [&](GNode n)
        {
          auto ei = bgraph.edge_begin(n);
          auto ee = bgraph.edge_end(n);

          uint64_t curMaster = master_partition[n];
          GNode lid;

          if (curMaster == 0)
          {
            lid = gid_to_lid[n];
            out_degrees[lid] = std::distance(ei, ee);

            for (; ei != ee; ++ei)
            {
              GNode dst = bgraph.getEdgeDst(ei);
              if (coverage_vector[lid] == false && master_partition[dst] != curMaster)
              {
                local_lock.lock();
                coverage_vector[lid] = false;
                local_lock.unlock();
              }
            }
          }
          else
          {
            lid = gid_to_lids[curMaster][n];
            out_degree_vectors[curMaster][lid] = std::distance(ei, ee);

            for (; ei != ee; ++ei)
            {
              GNode dst = bgraph.getEdgeDst(ei);
              if (coverage_vectors[curMaster][lid] == false && master_partition[dst] != curMaster)
              {
                lock.lock();
                coverage_vectors[curMaster][lid] = false;
                lock.unlock();
              }
            }
          }
        },
        galois::loopname("Coverage Vector and Out Degree Vector"));

    // Send the Coverage Vectors and Out Degree Vectors
    for (int i = 1; i < num_compute; i++)
    {
      net.send(i, 0, coverage_vectors[i].data(), master_partition_sizes[i], MPI_CXX_BOOL);
      net.send(i, 0, out_degree_vectors[i].data(), master_partition_sizes[i], MPI_UINT64_T);

      coverage_vectors[i].deallocate();
      out_degree_vectors[i].deallocate();
    }
  }
  else
  {
    if (node_type == COMPUTE_NODE && node_id != 0)
    {
      std::vector<GNode> vbuffer;
      std::vector<GNode> ebuffer;
      vertexEdgeCount vec;

      vbuffer.resize(num_vertices);
      net.recv(0, 0, vbuffer.data(), num_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      for (uint64_t i = 0; i < num_vertices; i++)
      {
        gid_to_lid[vbuffer[i]] = i;
        lid_to_gid[i] = vbuffer[i];
      }
      spdlog::debug("[Proc {}] Received vertices {}", node_id, fmt_array(vbuffer));

      coverage_vector.allocateLocal(num_vertices);
      out_degrees.allocateLocal(num_vertices);

      net.recv(0, 0, coverage_vector.data(), num_vertices, MPI_CXX_BOOL, MPI_STATUS_IGNORE);
      net.recv(0, 0, out_degrees.data(), num_vertices, MPI_UINT64_T, MPI_STATUS_IGNORE);

      spdlog::info("[Proc {}] Received the coverage vector and out degree vectors", node_id);
    }

    if (node_type == MEMORY_NODE)
    {
      std::vector<GNode> vbuffer;
      std::vector<uint64_t> edge_ends_buffer;
      uint64_t ecount = 0;

      vbuffer.resize(num_vertices);
      edge_ends_buffer.resize(num_vertices);

      net.recv(0, 0, vbuffer.data(), num_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, edge_ends_buffer.data(), num_vertices, MPI_UINT64_T, MPI_STATUS_IGNORE);
      for (uint64_t i = 0; i < num_vertices; i++)
      {
        gid_to_lid[vbuffer[i]] = i;
        lid_to_gid[i] = vbuffer[i];
        ecount += edge_ends_buffer[i];
        lgraph.fixEndEdge(i, ecount);
      }
      spdlog::debug("[Proc {}] Received vertices {}", node_id, fmt_array(vbuffer));

      uint64_t processedVertices = 0;
      uint64_t currLVertex = 0;
      uint64_t cur = 0;
      uint64_t vcount = num_vertices;

      /*
        vertexEdgeCount vec;
        std::vector<GNode> ebuffer;
        MPI_Request vec_mpi_buffer = MPI_REQUEST_NULL;
        MPI_Request edge_mpi_buffer = MPI_REQUEST_NULL;

        while (processedVertices != vcount)
        {
          net.recv(0, 0, &vec, 1, vertexEdgeCountType, MPI_STATUS_IGNORE);

          currLVertex = gid_to_lid[vec.vertex];
          ebuffer.resize(vec.edge_count);

          spdlog::debug(
              "[Proc {}] Receiving vertex {} with {}/{} edges from the coordinator",
              node_id,
              vec.vertex,
              vec.edge_count,
              ebuffer.size());

          net.recv(0, 0, ebuffer.data(), vec.edge_count, MPI_GNODE_T, MPI_STATUS_IGNORE);

          cur = *lgraph.edge_begin(currLVertex, galois::MethodFlag::UNPROTECTED);
          for (GNode& dst : ebuffer)
          {
            lgraph.constructEdge(cur++, gid_to_lid[dst]);
          }

          spdlog::debug(
              "[Proc {}] Processed {}/{} Edges for Vertex {}", node_id, cur, *lgraph.edge_end(currLVertex), vec.vertex);
          assert(cur == *lgraph.edge_end(currLVertex));

          ebuffer.clear();
          processedVertices++;
        }
        */

      galois::substrate::SimpleLock lock;
      galois::graphs::readGraph(bgraph, graph_path);
      spdlog::info("Read {} nodes and {} edges", bgraph.size(), bgraph.sizeEdges());
      galois::do_all(
          galois::iterate(lgraph),
          [&](GNode n)
          {
            GNode gnode = vbuffer[n];
            auto ii = bgraph.edge_begin(gnode);
            auto ee = bgraph.edge_end(gnode);

            uint64_t cur = *lgraph.edge_begin(n, galois::MethodFlag::UNPROTECTED);
            for (; ii < ee; ++ii)
            {
              GNode dst = bgraph.getEdgeDst(ii);
              if (gid_to_lid.find(dst) == gid_to_lid.end())
              {
                lock.lock();
                lid_to_gid[num_vertices] = dst;
                gid_to_lid[dst] = num_vertices++;
                master_partition_sizes[master_partition[dst]]++;  // Account for the additional mirrors
                lock.unlock();
              }
              lgraph.constructEdge(cur++, gid_to_lid[dst]);
            }
            spdlog::debug("[Proc {}] Processed {}/{} Edges for Vertex {}", node_id, cur, *lgraph.edge_end(n), gnode);
            assert(cur == *lgraph.edge_end(n));
          },
          galois::loopname("Distribute Vertices to Mirrors"));

      bgraph.deallocate();

      spdlog::debug("[Proc {}] Received the vertices/edges from the coordinator", node_id);
    }
  }

  net.barrier();
  // Print the Local Graph
  if (node_type == MEMORY_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices and {} edges", node_id, lgraph.size(), lgraph.sizeEdges());
    // Print Edges
    galois::do_all(
        galois::iterate(lgraph),
        [&](GNode n)
        {
          for (auto ii = lgraph.edge_begin(n), ei = lgraph.edge_end(n); ii != ei; ++ii)
          {
            GNode dst = lgraph.getEdgeDst(ii);
            spdlog::debug("[Proc {}] Edge {} -> {}", node_id, n, dst);
          }
        },
        galois::loopname("Print Local Graph"));
  }
  if (node_type == COMPUTE_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices", node_id, lgraph.size());
  }

  // TODO: Validate Address Translation Table
  if (node_type == MEMORY_NODE)
  {
    addrTranslationTable.resize(num_compute);
    std::vector<uint64_t> addrTranslationTableSizes(num_compute, 0);
    for (int i = 0; i < num_compute; i++)
    {
      addrTranslationTable[i].allocateLocal(master_partition_sizes[i]);
    }

    galois::substrate::SimpleLock lock;
    galois::do_all(
        galois::iterate(uint64_t(0), num_vertices),
        [&](uint64_t n)
        {
          GNode gid = lid_to_gid[n];
          uint64_t masterID = master_partition[gid];
          addrTranslationTable[masterID][addrTranslationTableSizes[masterID]] = n;

          lock.lock();
          addrTranslationTableSizes[masterID]++;
          lock.unlock();
        },
        galois::loopname("Memory Node: Create Address Translation Table"));
    addrTranslationTableSizes.clear();
  }
  else if (node_type == COMPUTE_NODE)
  {
    addrTranslationTable.resize(num_memory);
    std::vector<uint64_t> addrTranslationTableSizes(num_memory, 0);
    for (int i = 0; i < num_memory; i++)
    {
      addrTranslationTable[i].allocateLocal(mirror_partition_sizes[i]);
    }

    galois::substrate::SimpleLock lock;
    galois::do_all(
        galois::iterate(uint64_t(0), num_vertices),
        [&](uint64_t n)
        {
          GNode gid = lid_to_gid[n];
          uint64_t mirrorID = mirror_partition[gid];
          addrTranslationTable[mirrorID][addrTranslationTableSizes[mirrorID]] = n;

          lock.lock();
          addrTranslationTableSizes[mirrorID]++;
          lock.unlock();
          spdlog::debug("[Proc {}] Adding Vertex {} to Translation Table/Mirror {}", node_id, n, mirrorID);
        },
        galois::loopname("Compute Node: Create Address Translation Table"));

    addrTranslationTableSizes.clear();
  }

  if (node_type == MEMORY_NODE)
  {
    bitCommVector.resize(num_compute);
    for (int i = 0; i < num_compute; i++)
    {
      bitCommVector[i].resize(master_partition_sizes[i]);
    }
  }
  else if (node_type == COMPUTE_NODE)
  {
    bitCommVector.resize(num_memory);
    for (int i = 0; i < num_memory; i++)
    {
      bitCommVector[i].resize(mirror_partition_sizes[i]);
    }
  }

  net.barrier();

  // FIXME: Clear all Memory Allocations
}

bool DistributedGraph::isCoverageComplete(std::vector<GNode>& frontier)
{
  bool coverage = true;
  for (GNode& n : frontier)
  {
    if (coverage_vector[n] == false)
    {
      coverage = false;
      break;
    }
  }
  return coverage;
}

GNode DistributedGraph::getLocalNode(GNode& gid)
{
  return gid_to_lid[gid];
}

GNode DistributedGraph::getGlobalNode(GNode& lid)
{
  return lid_to_gid[lid];
}

uint64_t DistributedGraph::getOutDegree(GNode& lid)
{
  return out_degrees[lid];
}

uint64_t DistributedGraph::getMirrorPartition(GNode& gid)
{
  return mirror_partition[gid];
}

uint64_t DistributedGraph::getMasterPartition(GNode& gid)
{
  return master_partition[gid];
}

DistributedGraph::~DistributedGraph()
{
}
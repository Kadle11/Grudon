
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
    std::string& partitioning_scheme_file)
    : num_compute(num_compute),
      num_memory(num_memory),
      node_id(node_id),
      node_type(node_type),
      bitCommVector_Send(bitCommVector_Send),
      bitCommVector_Recv(bitCommVector_Recv),
      sTranslationTable(sTranslationTable),
      rTranslationTable(rTranslationTable),
      sAggrTranslationTable(sAggrTranslationTable),
      out_degrees(out_degrees),
      coverage_vector(coverage_vector),
      num_vertices(num_vertices),
      total_vertices(total_vertices),
      num_edges(num_edges),
      net(net)
{
  // spdlog::set_level(spdlog::level::debug);

  std::vector<uint64_t> mirror_edge_counts;
  if (node_id == 0)
  {
    // Read the Graph
    galois::graphs::readGraph(bgraph, graph_path);

    avg_degree = bgraph.sizeEdges() / bgraph.size();

    spdlog::info("Read {} nodes and {} edges", bgraph.size(), bgraph.sizeEdges());

    // Partition the Graph
    master_partition_sizes.resize(num_compute, 0);
    mirror_partition_sizes.resize(num_memory, 0);
    mirror_edge_counts.resize(num_memory, 0);

    METISPartitioner(
        bgraph,
        num_compute,
        num_memory,
        mirror_partition,
        master_partition,
        master_partition_sizes,
        mirror_partition_sizes,
        mirror_edge_counts,
        partitioning_scheme_file);

    spdlog::info("Partitioned the across {} masters and {} mirrors", num_compute, num_memory);
  }

  // Create the Address Translation Table
  std::vector<uint64_t> translationTableSizes;
  std::vector<galois::DynamicBitSet> cross_node_mirrors;
  std::vector<GNode> vbuffer;

  // Distribute the number of vertices to all nodes
  num_edges = 0;
  total_vertices = 0;
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

    for (int i = 0; i < num_memory; i++)
    {
      net.send(i + num_compute, 0, &mirror_partition_sizes[i], 1, MPI_UINT64_T);
      net.send(i + num_compute, 0, &total_vertices, 1, MPI_UINT64_T);
      net.send(i + num_compute, 0, master_partition.data(), total_vertices, MPI_GNODE_T);
      net.send(i + num_compute, 0, master_partition_sizes.data(), num_compute, MPI_UINT64_T);
      net.send(i + num_compute, 0, &mirror_edge_counts[i], 1, MPI_UINT64_T);
    }

    translationTableSizes.resize(num_memory, 0);
    cross_node_mirrors.resize(num_memory);
  }
  else
  {
    net.recv(0, 0, &num_vertices, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);
    net.recv(0, 0, &total_vertices, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);

    if (node_type == COMPUTE_NODE)
    {
      mirror_partition.allocateLocal(total_vertices);
      mirror_partition_sizes.resize(num_memory);
      translationTableSizes.resize(num_memory, 0);
      cross_node_mirrors.resize(num_memory);

      net.recv(0, 0, mirror_partition.data(), total_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, mirror_partition_sizes.data(), num_memory, MPI_UINT64_T, MPI_STATUS_IGNORE);
    }
    else if (node_type == MEMORY_NODE)
    {
      master_partition.allocateLocal(total_vertices);
      master_partition_sizes.resize(num_compute);
      translationTableSizes.resize(num_compute, 0);
      cross_node_mirrors.resize(1);

      net.recv(0, 0, master_partition.data(), total_vertices, MPI_GNODE_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, master_partition_sizes.data(), num_compute, MPI_UINT64_T, MPI_STATUS_IGNORE);
      net.recv(0, 0, &num_edges, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);
    }
  }

  spdlog::info("[Proc {}] Allocating the graph with {} vertices and {} edges", node_id, num_vertices, num_edges);

  net.barrier();
  lgraph.allocateFrom(num_vertices, num_edges);
  lgraph.constructNodes();

  lid_to_gid.resize(total_vertices);
  gid_to_lid.resize(total_vertices);

  if (node_type == COMPUTE_NODE)
  {
    coverage_vector.allocateLocal(num_vertices);
    out_degrees.allocateLocal(num_vertices);
  }

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
              if (master_partition[n] == 0)
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
              if (master_partition[n] == i)
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
    for (int i = 0; i < num_memory; i++)
    {
      cross_node_mirrors[i].resize(total_vertices);
    }

    galois::GAccumulator<uint64_t> total_cross_node_mirrors;
    total_cross_node_mirrors.reset();

    galois::do_all(
        galois::iterate(bgraph),
        [&](GNode n)
        {
          auto ei = bgraph.edge_begin(n);
          auto ee = bgraph.edge_end(n);

          uint64_t curMaster = master_partition[n];
          uint64_t curMirror = mirror_partition[n];
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

              if (mirror_partition[dst] != curMirror)
              {
                cross_node_mirrors[curMirror].set(dst);
                // total_cross_node_mirrors += 1;
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

              if (mirror_partition[dst] != curMirror)
              {
                cross_node_mirrors[curMirror].set(dst);
                // total_cross_node_mirrors += 1;
              }
            }
          }
        },
        galois::loopname("Coverage Vector and Out Degree Vector"));

    spdlog::debug(
        "[Proc {}] Coverage vector: {}, Out Degree vector: {}", node_id, fmt_array(coverage_vector), fmt_array(out_degrees));

    // Send the Cross Node Mirrors
    size_t cross_node_mirrors_size = cross_node_mirrors[0].size_bytes();
    for (int i = 0; i < num_memory; i++)
    {
      net.send(i + num_compute, 0, cross_node_mirrors[i].bitvec.data(), cross_node_mirrors_size, MPI_UINT64_T);
    }

    // Share all the Cross Node Mirrors to all Compute Nodes
    for (int i = 1; i < num_compute; i++)
    {
      for (int j = 0; j < num_memory; j++)
      {
        net.send(i, 0, cross_node_mirrors[j].bitvec.data(), cross_node_mirrors_size, MPI_UINT64_T);
      }
    }

    // Send the Coverage Vectors and Out Degree Vectors
    for (int i = 1; i < num_compute; i++)
    {
      net.send(i, 0, coverage_vectors[i].data(), master_partition_sizes[i], MPI_CXX_BOOL);
      net.send(i, 0, out_degree_vectors[i].data(), master_partition_sizes[i], MPI_UINT64_T);

      coverage_vectors[i].deallocate();
      out_degree_vectors[i].deallocate();
    }

    for (uint64_t i = 0; i < num_vertices; i++)
    {
      GNode gnode = lid_to_gid[i];
      const uint32_t& mem_node = mirror_partition[gnode];
      translationTableSizes[mem_node]++;

      for (int j = 0; j < num_memory; j++)
      {
        if (j != mem_node && cross_node_mirrors[j].test(gnode))
        {
          translationTableSizes[j]++;
        }
      }
    }

    galois::LargeArray<GNode> new_master_partition;
    galois::LargeArray<GNode> new_mirror_partition;

    new_master_partition.allocateLocal(num_vertices);
    new_mirror_partition.allocateLocal(num_vertices);

    for (GNode lnode = 0; lnode < num_vertices; ++lnode)
    {
      GNode gnode = lid_to_gid[lnode];
      GNode masterID = master_partition[gnode];
      GNode mirrorID = mirror_partition[gnode];

      new_master_partition[lnode] = masterID;
      new_mirror_partition[lnode] = mirrorID;
    }

    master_partition.deallocate();
    mirror_partition.deallocate();

    master_partition.allocateLocal(num_vertices);
    mirror_partition.allocateLocal(num_vertices);

    std::copy(new_master_partition.begin(), new_master_partition.end(), master_partition.begin());
    std::copy(new_mirror_partition.begin(), new_mirror_partition.end(), mirror_partition.begin());

    new_master_partition.deallocate();
    new_mirror_partition.deallocate();

    uint64_t global_cross_node_mirrors = std::accumulate(
        cross_node_mirrors.begin(),
        cross_node_mirrors.end(),
        0,
        [](uint64_t a, galois::DynamicBitSet& b) { return a + b.count(); });

    replication_factor = float(global_cross_node_mirrors + bgraph.size()) / bgraph.size();
    galois::runtime::reportStat_Single("Partitioning Scheme", "Replication Factor", replication_factor);
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
      spdlog::debug("[Proc {}] Received vertices {}", node_id, fmt_array(vbuffer));

      for (int i = 0; i < num_memory; i++)
      {
        cross_node_mirrors[i].resize(total_vertices);
      }

      // Recv the Cross Node Mirrors
      size_t cross_node_mirrors_size = cross_node_mirrors[0].size_bytes();
      for (int i = 0; i < num_memory; i++)
      {
        net.recv(0, 0, cross_node_mirrors[i].bitvec.data(), cross_node_mirrors_size, MPI_UINT64_T, MPI_STATUS_IGNORE);
      }

      for (uint64_t i = 0; i < num_vertices; i++)
      {
        gid_to_lid[vbuffer[i]] = i;
        lid_to_gid[i] = vbuffer[i];

        const uint32_t& mem_node = mirror_partition[vbuffer[i]];
        translationTableSizes[mem_node]++;

        for (int j = 0; j < num_memory; j++)
        {
          if (j != mem_node && cross_node_mirrors[j].test(vbuffer[i]))
          {
            translationTableSizes[j]++;
          }
        }
      }

      net.recv(0, 0, coverage_vector.data(), num_vertices, MPI_CXX_BOOL, MPI_STATUS_IGNORE);
      net.recv(0, 0, out_degrees.data(), num_vertices, MPI_UINT64_T, MPI_STATUS_IGNORE);

      spdlog::debug(
          "[Proc {}] Coverage vector: {}, Out Degree vector: {}",
          node_id,
          fmt_array(coverage_vector),
          fmt_array(out_degrees));
      spdlog::info("[Proc {}] Received the coverage vector and out degree vectors", node_id);
    }

    if (node_type == MEMORY_NODE)
    {
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

        translationTableSizes[master_partition[vbuffer[i]]]++;
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
      spdlog::info("[Proc{}] Read {} nodes and {} edges", node_id, bgraph.size(), bgraph.sizeEdges());

      // Recv the Cross Node Mirrors
      cross_node_mirrors[0].resize(total_vertices);
      net.recv(
          0, 0, cross_node_mirrors[0].bitvec.data(), cross_node_mirrors[0].size_bytes(), MPI_UINT64_T, MPI_STATUS_IGNORE);

      for (GNode n = 0; n < total_vertices; ++n)
      {
        if (cross_node_mirrors[0].test(n))
        {
          translationTableSizes[master_partition[n]]++;
          lid_to_gid[num_vertices] = n;
          gid_to_lid[n] = num_vertices++;
          vbuffer.push_back(n);
        }
      }

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
              GNode lid = gid_to_lid[dst];
              lock.lock();
              lgraph.constructEdge(cur, lid);
              lock.unlock();
              cur++;
            }
            spdlog::debug("[Proc {}] Processed {}/{} Edges for Vertex {}", node_id, cur, *lgraph.edge_end(n), gnode);
            assert(cur == *lgraph.edge_end(n));
          },
          galois::loopname("Distribute Vertices to Mirrors"));

      bgraph.deallocate();

      spdlog::info("[Proc {}] Created Local Graph", node_id);
    }
  }

  net.barrier();
  // Print the Local Graph
  if (node_type == MEMORY_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices and {} edges", node_id, lgraph.size(), lgraph.sizeEdges());
  }
  else if (node_type == COMPUTE_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices", node_id, lgraph.size());
  }

  if (node_type == MEMORY_NODE)
  {
    sTranslationTable.resize(num_compute);
    rTranslationTable.resize(num_compute);
    sAggrTranslationTable.resize(num_compute);
    std::vector<uint64_t> addrTranslationTableIdxs(num_compute, 0);
    std::vector<uint64_t> aggrAddrTranslationTableIdxs(num_compute, 0);

    // Reserve the Translation Table Sizes
    for (size_t i = 0; i < num_compute; i++)
    {
      sTranslationTable[i].resize(total_vertices);
      rTranslationTable[i].resize(total_vertices);
      sAggrTranslationTable[i].reserve(master_partition_sizes[i]);
    }

    // std::sort(vbuffer.begin(), vbuffer.end());
    galois::DynamicBitSet vbufferBitset;
    vbufferBitset.resize(master_partition.size());
    for (GNode gnode : vbuffer)
    {
      GNode lnode = gid_to_lid[gnode];
      uint64_t masterID = master_partition[gnode];
      rTranslationTable[masterID][addrTranslationTableIdxs[masterID]] = lnode;
      sTranslationTable[masterID][lnode] = addrTranslationTableIdxs[masterID];

      // spdlog::info(
      //     "[Proc {}] LNode/MirrorIndex/GNode: {}/{}/{} for Compute Node {}",
      //     node_id,
      //     lnode,
      //     addrTranslationTableIdxs[masterID],
      //     gnode,
      //     masterID);

      addrTranslationTableIdxs[masterID]++;
      vbufferBitset.set(gnode);
    }

    for (int64_t gnode = 0; gnode < master_partition.size(); gnode++)
    {
      uint64_t masterID = master_partition[gnode];

      if (vbufferBitset.test(gnode))
      {
        GNode lnode = gid_to_lid[gnode];
        sAggrTranslationTable[masterID][lnode] = aggrAddrTranslationTableIdxs[masterID];
      }
      aggrAddrTranslationTableIdxs[masterID]++;
    }

    for (int i = 0; i < num_compute; i++)
    {
      assert(addrTranslationTableIdxs[i] == translationTableSizes[i]);
    }
    aggrAddrTranslationTableIdxs.clear();
    addrTranslationTableIdxs.clear();
    vbuffer.clear();

    // Re-write the master/mirror partitions using the local node ids

    galois::LargeArray<GNode> new_master_partition;
    new_master_partition.allocateLocal(num_vertices);

    for (GNode lnode = 0; lnode < num_vertices; ++lnode)
    {
      GNode gnode = lid_to_gid[lnode];
      uint64_t masterID = master_partition[gnode];
      new_master_partition[lnode] = masterID;
    }

    master_partition.deallocate();
    master_partition.allocateLocal(num_vertices);

    std::copy(new_master_partition.begin(), new_master_partition.end(), master_partition.begin());
    new_master_partition.deallocate();
  }
  else if (node_type == COMPUTE_NODE)
  {
    std::vector<uint64_t> addrTranslationTableIdxs(num_memory, 0);
    rTranslationTable.resize(num_memory);
    sTranslationTable.resize(num_memory);

    // Reserve the Translation Table Sizes
    for (size_t i = 0; i < num_memory; i++)
    {
      sTranslationTable[i].resize(total_vertices);
      rTranslationTable[i].resize(total_vertices);
    }

    for (GNode lnode = 0; lnode < num_vertices; ++lnode)
    {
      GNode gnode = lid_to_gid[lnode];
      uint64_t mirrorID = mirror_partition[gnode];
      rTranslationTable[mirrorID][addrTranslationTableIdxs[mirrorID]] = lnode;
      sTranslationTable[mirrorID][lnode] = addrTranslationTableIdxs[mirrorID];

      // spdlog::info(
      //     "[Proc {}] LNode/MirrorIndex/GNode: {}/{}/{} for Memory Node {}",
      //     node_id,
      //     lnode,
      //     addrTranslationTableIdxs[mirrorID],
      //     gnode,
      //     mirrorID);

      addrTranslationTableIdxs[mirrorID]++;
    }

    for (GNode lnode = 0; lnode < num_vertices; ++lnode)
    {
      GNode gnode = lid_to_gid[lnode];
      uint64_t mirrorID = mirror_partition[gnode];

      // Check for Cross Node Mirrors
      for (int i = 0; i < num_memory; i++)
      {
        if (i != mirrorID && cross_node_mirrors[i].test(gnode))
        {
          rTranslationTable[i][addrTranslationTableIdxs[i]] = lnode;
          sTranslationTable[i][lnode] = addrTranslationTableIdxs[i];
          addrTranslationTableIdxs[i]++;
        }
      }
    }

    for (int i = 0; i < num_memory; i++)
    {
      assert(addrTranslationTableIdxs[i] == translationTableSizes[i]);
    }

    addrTranslationTableIdxs.clear();

    galois::LargeArray<GNode> new_mirror_partition;
    new_mirror_partition.allocateLocal(num_vertices);

    for (GNode lnode = 0; lnode < num_vertices; ++lnode)
    {
      GNode gnode = lid_to_gid[lnode];
      GNode mirrorID = mirror_partition[gnode];
      new_mirror_partition[lnode] = mirrorID;
    }

    mirror_partition.deallocate();
    mirror_partition.allocateLocal(num_vertices);

    std::copy(new_mirror_partition.begin(), new_mirror_partition.end(), mirror_partition.begin());
    new_mirror_partition.deallocate();
  }

  if (node_type == MEMORY_NODE)
  {
    bitCommVector_Send.resize(num_compute);
    bitCommVector_Recv.resize(num_compute);
    for (int i = 0; i < num_compute; i++)
    {
      bitCommVector_Send[i].resize(master_partition_sizes[i]);
      bitCommVector_Recv[i].resize(translationTableSizes[i]);
    }
  }
  else if (node_type == COMPUTE_NODE)
  {
    bitCommVector_Send.resize(num_memory);
    bitCommVector_Recv.resize(num_memory);
    for (int i = 0; i < num_memory; i++)
    {
      bitCommVector_Send[i].resize(translationTableSizes[i]);
      bitCommVector_Recv[i].resize(num_vertices);
    }
  }

  net.barrier();

  // FIXME: Clear all Memory Allocations

  if (node_id == 0)
  {
    bgraph.deallocate();
  }
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

GNode DistributedGraph::getLocalNode(const GNode& gid)
{
  return gid_to_lid[gid];
}

GNode DistributedGraph::getGlobalNode(const GNode& lid)
{
  return lid_to_gid[lid];
}

uint64_t DistributedGraph::getOutDegree(const GNode& lid)
{
  return out_degrees[lid];
}

uint64_t DistributedGraph::getMirrorPartition(const GNode& lid)
{
  return mirror_partition[lid];
}

uint64_t DistributedGraph::getMasterPartition(const GNode& lid)
{
  return master_partition[lid];
}

DistributedGraph::~DistributedGraph()
{
}

void DistributedGraph::printGraph()
{
  if (node_type == MEMORY_NODE)
  {
    // Print Edges
    galois::do_all(
        galois::iterate(lgraph),
        [&](GNode n)
        {
          for (auto ii = lgraph.edge_begin(n), ei = lgraph.edge_end(n); ii != ei; ++ii)
          {
            GNode dst = lgraph.getEdgeDst(ii);
            spdlog::info("[Proc {}] Edge {} -> {}", node_id, n, dst);
          }
        },
        galois::loopname("Print Local Graph"));
  }
}

void DistributedGraph::printState()
{
  if (node_type == COMPUTE_NODE)
  {
    spdlog::info("[Proc {}] Node Type: Compute Node", node_id);
  }
  else if (node_type == MEMORY_NODE)
  {
    spdlog::info("[Proc {}] Node Type: Memory Node", node_id);
  }
  spdlog::info("[Proc {}] Number of Vertices: {}", node_id, num_vertices);
  spdlog::info("[Proc {}] Number of Compute Nodes: {}", node_id, num_compute);
  spdlog::info("[Proc {}] Number of Memory Nodes: {}", node_id, num_memory);
  spdlog::info("[Proc {}] Master Partition Sizes: {}", node_id, fmt_array(master_partition_sizes));
  spdlog::info("[Proc {}] Mirror Partition Sizes: {}", node_id, fmt_array(mirror_partition_sizes));

  spdlog::info("[Proc {}] Master Partition: {}", node_id, fmt_array(master_partition));
  spdlog::info("[Proc {}] Mirror Partition: {}", node_id, fmt_array(mirror_partition));

  spdlog::info("[Proc {}] Address Translation Table: ", node_id);

  if (node_type == MEMORY_NODE)
  {
    // Print Edges
    galois::do_all(
        galois::iterate(lgraph),
        [&](GNode n)
        {
          for (auto ii = lgraph.edge_begin(n), ei = lgraph.edge_end(n); ii != ei; ++ii)
          {
            GNode dst = lgraph.getEdgeDst(ii);
            spdlog::info("[Proc {}] Edge {} -> {}", node_id, n, dst);
          }
        },
        galois::loopname("Print Local Graph"));

    for (int i = 0; i < num_compute; i++)
    {
      for (auto& translationElem : sTranslationTable[i])
      {
        spdlog::info(
            "[Proc {}] Memory Node {}: GNode {} -> Set Bit {}", node_id, i, lid_to_gid[translationElem], translationElem);
      }
    }

    for (int i = 0; i < num_compute; i++)
    {
      for (auto& translationElem : rTranslationTable[i])
      {
        spdlog::info(
            "[Proc {}] Memory Node {}: Test Bit {} -> GNode {}", node_id, i, translationElem, lid_to_gid[translationElem]);
      }
    }
  }
  else if (node_type == COMPUTE_NODE)
  {
    for (int i = 0; i < num_memory; i++)
    {
      for (auto& translationElem : sTranslationTable[i])
      {
        spdlog::info(
            "[Proc {}] Compute Node {}: GNode {} -> Set Bit {}", node_id, i, lid_to_gid[translationElem], translationElem);
      }
    }

    for (int i = 0; i < num_memory; i++)
    {
      for (auto& translationElem : rTranslationTable[i])
      {
        spdlog::info(
            "[Proc {}] Compute Node {}: Test Bit {} -> GNode {}", node_id, i, translationElem, lid_to_gid[translationElem]);
      }
    }
  }
}

double calculateSkew(std::vector<GNode>& frontier, uint32_t& num_memory, DistributedGraph& distributed_graph)
{
  std::vector<galois::GAccumulator<uint64_t>> mirrorCoverage(num_memory);
  for (auto& mc : mirrorCoverage)
  {
    mc.reset();
  }

  galois::do_all(
      galois::iterate(frontier.begin(), frontier.end()),
      [&](const GNode& lid)
      {
        uint32_t worker_id = distributed_graph.getMirrorPartition(lid);
        mirrorCoverage[worker_id] += 1;
      },
      galois::loopname("Mirror Coverage"),
      galois::no_stats(),
      galois::steal());

  std::vector<uint64_t> mirrorCoverageVector;
  mirrorCoverageVector.reserve(num_memory);
  for (auto& mc : mirrorCoverage)
  {
    mirrorCoverageVector.push_back(mc.reduce());
  }

  double avgMirrorCoverage = std::accumulate(mirrorCoverageVector.begin(), mirrorCoverageVector.end(), 0) / num_memory;
  double variance = std::accumulate(
      mirrorCoverageVector.begin(),
      mirrorCoverageVector.end(),
      0.0,
      [&](double a, uint64_t b) { return a + std::pow((b - avgMirrorCoverage), 2); });

  double stddev = std::sqrt(variance / num_memory);

  double skewness = std::accumulate(
      mirrorCoverageVector.begin(),
      mirrorCoverageVector.end(),
      0,
      [&](double a, uint64_t b) { return a + std::pow((b - avgMirrorCoverage), 3); });

  if (stddev == 0)
  {
    return 0;
  }

  spdlog::info(
      "Average Mirror Coverage: {}, Variance: {}, StdDev: {}, Skewness: {}", avgMirrorCoverage, variance, stddev, skewness);

  skewness /= (num_memory * std::pow(stddev, 3));

  return skewness;
}
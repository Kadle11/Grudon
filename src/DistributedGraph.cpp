
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
  if (node_id == 0)
  {
    num_vertices = master_partition_sizes[0];
    for (int i = 1; i < num_compute; i++)
    {
      net.send(i, 0, &master_partition_sizes[i], 1, MPI_UINT64_T);
    }

    for (int i = num_compute; i < num_compute + num_memory; i++)
    {
      net.send(i, 0, &mirror_partition_sizes[i - num_compute], 1, MPI_UINT64_T);
      net.send(i, 0, &mirror_edge_counts[i - num_compute], 1, MPI_UINT64_T);
    }
  }
  else
  {
    net.recv(0, 0, &num_vertices, 1, MPI_UINT64_T, MPI_STATUS_IGNORE);

    if (node_type == MEMORY_NODE)
    {
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

    galois::do_all(
        galois::iterate(size_t(1), num_compute),
        [&](size_t i)
        {
          std::vector<GNode> vbuffer;
          vbuffer.reserve(master_partition_sizes[i]);
          for (GNode n = 0; n < master_partition.size(); ++n)
          {
            if (n % num_compute == i)
            {
              vbuffer.push_back(n);
            }
          }

          spdlog::debug(
              "[Proc {}] Sending vertices {}/{} to compute node {}", node_id, vbuffer.size(), master_partition_sizes[i], i);

          assert(vbuffer.size() == master_partition_sizes[i]);

          lock.lock();
          net.send(i, 0, vbuffer.data(), vbuffer.size(), MPI_GNODE_T);
          lock.unlock();

          vbuffer.clear();
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
          net.isend(mirror_partition[n] + num_compute, 0, &vecSend, 1, vertexEdgeCountType, &vec_mpi_buffer);
          net.isend(mirror_partition[n] + num_compute, 0, ebuffer.data(), ebuffer.size(), MPI_GNODE_T, &edge_mpi_buffer);
          lock.unlock();

          ebuffer.clear();
        },
        galois::chunk_size<1024>(),
        galois::loopname("Distribute Vertices to Mirrors"));
    spdlog::info("[Proc {}] Distributed the edges to all memory nodes", node_id);
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
      }
      spdlog::debug("[Proc {}] Received vertices {}", node_id, fmt_array(vbuffer));
    }

    if (node_type == MEMORY_NODE)
    {
      galois::substrate::SimpleLock lock;
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
        ecount += edge_ends_buffer[i];
        lgraph.fixEndEdge(i, ecount);
      }
      spdlog::debug("[Proc {}] Received vertices {}", node_id, fmt_array(vbuffer));

      uint64_t processedVertices = 0;
      uint64_t currLVertex = 0;
      uint64_t cur = 0;
      uint64_t vcount = num_vertices;

      galois::do_all(
          galois::iterate(uint64_t{ 0 }, num_vertices),
          [&](uint64_t pv)
          {
            std::vector<GNode> ebuffer;
            vertexEdgeCount vec;

            MPI_Request vec_mpi_buffer = MPI_REQUEST_NULL;
            MPI_Request edge_mpi_buffer = MPI_REQUEST_NULL;

            lock.lock();
            net.irecv(0, 0, &vec, 1, vertexEdgeCountType, &vec_mpi_buffer);
            MPI_Wait(&vec_mpi_buffer, MPI_STATUS_IGNORE);

            currLVertex = gid_to_lid[vec.vertex];
            ebuffer.resize(vec.edge_count);

            net.irecv(0, 0, ebuffer.data(), vec.edge_count, MPI_GNODE_T, &edge_mpi_buffer);
            MPI_Wait(&edge_mpi_buffer, MPI_STATUS_IGNORE);
            lock.unlock();

            cur = *lgraph.edge_begin(currLVertex, galois::MethodFlag::UNPROTECTED);
            for (GNode& dst : ebuffer)
            {
              if (gid_to_lid.find(dst) == gid_to_lid.end())
              {
                gid_to_lid[dst] = num_vertices++;
              }
              lgraph.constructEdge(cur++, gid_to_lid[dst]);
            }

            ebuffer.clear();
          });

      /*
            while (processedVertices != vcount)
            {
              net.irecv(0, 0, &vec, 1, vertexEdgeCountType, &vec_mpi_buffer);
              MPI_Wait(&vec_mpi_buffer, MPI_STATUS_IGNORE);

              currLVertex = gid_to_lid[vec.vertex];
              ebuffer.resize(vec.edge_count);

              spdlog::debug(
                  "[Proc {}] Receiving vertex {} with {}/{} edges from the coordinator",
                  node_id,
                  vec.vertex,
                  vec.edge_count,
                  ebuffer.size());

              net.irecv(0, 0, ebuffer.data(), vec.edge_count, MPI_GNODE_T, &edge_mpi_buffer);
              MPI_Wait(&edge_mpi_buffer, MPI_STATUS_IGNORE);

              cur = *lgraph.edge_begin(currLVertex, galois::MethodFlag::UNPROTECTED);
              for (GNode& dst : ebuffer)
              {
                if (gid_to_lid.find(dst) == gid_to_lid.end())
                {
                  gid_to_lid[dst] = num_vertices++;
                }
                lgraph.constructEdge(cur++, gid_to_lid[dst]);
              }

              spdlog::debug(
                  "[Proc {}] Processed {}/{} Edges for Vertex {}", node_id, cur, *lgraph.edge_end(currLVertex), vec.vertex);
              assert(cur == *lgraph.edge_end(currLVertex));

              ebuffer.clear();
              processedVertices++;
            }
       */
      spdlog::debug("[Proc {}] Received the vertices/edges from the coordinator", node_id);
    }
  }

  net.barrier();
  // Print the Local Graph
  if (node_type == MEMORY_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices and {} edges", node_id, lgraph.size(), lgraph.sizeEdges());
  }
  if (node_type == COMPUTE_NODE)
  {
    spdlog::info("[Proc {}] Graph has {} vertices", node_id, lgraph.size());
  }

  // TODO: Create the Address Translation Table

  // TODO: Create the BitVector for Communication

  // FIXME: Clear all Memory Allocations
}

DistributedGraph::~DistributedGraph()
{
}
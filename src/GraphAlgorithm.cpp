#include "GraphAlgorithm.hpp"

#include <algorithm>

#include "Logger.hpp"
#include "MPI.hpp"
#include "offload_engine/INCEngine.hpp"
#include "offload_engine/NDPEngine.hpp"

template<typename VertexProperty>
void GraphAlgorithm<VertexProperty>::generatePerThreadMatrix(const std::vector<GNode> &vertices)
{
  size_t curr_vertices = vertices.size();
  verticesPerThread = 1 + ((curr_vertices > nGaloisThreads) ? curr_vertices / nGaloisThreads : 0);

  if (node_type == COMPUTE_NODE)
  {
    for (int i = 0; i < num_memory; i++)
    {
      perThreadVCounts[i].assign(nGaloisThreads, 0);
    }

    galois::do_all(
        galois::iterate(0ul, nGaloisThreads),
        [&](const size_t j)
        {
          const size_t &start = j * verticesPerThread;
          const size_t &end = (j + 1) * verticesPerThread > curr_vertices ? curr_vertices : (j + 1) * verticesPerThread;
          size_t pID;  // Partition ID

          auto &offsetMatrix = perThreadOffsetMatrix[j];

          for (size_t i = start; i < end; i++)
          {
            pID = worker->getVertexMemoryPartition(vertices[i]);
            offsetMatrix[pID][perThreadVCounts[pID][j]++] = vertices[i];
          }
        },
        galois::loopname("Generate Per Thread Matrix"),
        galois::no_stats(),
        galois::steal());
  }
  else if (node_type == MEMORY_NODE)
  {
    for (int i = 0; i < num_compute; i++)
    {
      perThreadVCounts[i].assign(nGaloisThreads, 0);
    }

    galois::do_all(
        galois::iterate(0ul, nGaloisThreads),
        [&](const size_t j)
        {
          const size_t &start = j * verticesPerThread;
          const size_t &end = (j + 1) * verticesPerThread > curr_vertices ? curr_vertices : (j + 1) * verticesPerThread;
          size_t pID;  // Partition ID

          auto &offsetMatrix = perThreadOffsetMatrix[j];

          for (size_t i = start; i < end; i++)
          {
            pID = worker->getVertexComputePartition(vertices[i]);
            offsetMatrix[pID][perThreadVCounts[pID][j]++] = vertices[i];
          }
        },
        galois::loopname("Generate Per Thread Matrix"),
        galois::no_stats(),
        galois::steal());
  }
}

template<typename VertexProperty>
GraphAlgorithm<VertexProperty>::GraphAlgorithm(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net,
    std::string &partitioning_scheme_file)
    : algorithm_name(algorithm_name),
      node_type(node_type),
      net(net),
      num_compute(num_compute),
      num_memory(num_memory),
      graph_path(graph_path)
{
  nGaloisThreads = galois::getActiveThreads();

  if (node_type == COMPUTE_NODE)
  {
    // nGaloisThreads /= 2;
    galois::setActiveThreads(nGaloisThreads);

    worker = new UpdateWorker<VertexProperty>(
        graph_path, num_compute, num_memory, node_id, node_type, net, partitioning_scheme_file);
    verticesPerThread = worker->num_vertices / nGaloisThreads > 0 ? worker->num_vertices / nGaloisThreads : 1;

    propertyBuffers.resize(num_memory);
    perThreadOffsetMatrix.resize(nGaloisThreads);
    perThreadVCounts.resize(num_memory);

    for (int i = 0; i < num_memory; i++)
    {
      propertyBuffers[i].allocateLocal(worker->sTranslationTable[i].size());
      perThreadVCounts[i].resize(nGaloisThreads, 0);
    }

    for (size_t i = 0; i < nGaloisThreads; i++)
    {
      perThreadOffsetMatrix[i] = std::vector<galois::LargeArray<GNode>>(num_memory);

      for (int j = 0; j < num_memory; j++)
      {
        perThreadOffsetMatrix[i][j].allocateLocal(verticesPerThread);
      }
    }
  }
  else if (node_type == MEMORY_NODE)
  {
    worker = new TraverseWorker<VertexProperty>(
        graph_path, num_compute, num_memory, node_id, node_type, net, partitioning_scheme_file);
    verticesPerThread = worker->num_vertices / nGaloisThreads > 0 ? worker->num_vertices / nGaloisThreads : 1;

    propertyBuffers.resize(num_compute);
    perThreadOffsetMatrix.resize(nGaloisThreads);
    perThreadVCounts.resize(num_compute);

    for (int i = 0; i < num_compute; i++)
    {
      propertyBuffers[i].allocateLocal(worker->sTranslationTable[i].size());
      perThreadVCounts[i].resize(nGaloisThreads, 0);
    }

    for (size_t i = 0; i < nGaloisThreads; i++)
    {
      perThreadOffsetMatrix[i] = std::vector<galois::LargeArray<GNode>>(num_compute);

      for (int j = 0; j < num_compute; j++)
      {
        perThreadOffsetMatrix[i][j].allocateLocal(verticesPerThread);
      }
    }
  }

  clear_updates = false;

  worker_completion_count = 0;
  num_workers = num_compute + num_memory;

  // frontier = galois::ThreadSafeOrderedSet<GNode>();
  frontier.resize(worker->num_vertices);
  vertex_properties.allocate(worker->num_vertices);
  vertex_updates.allocate(worker->num_vertices);
  MPI_VERTEX_PROPERTY_T = mpi_get_type<VertexProperty>();

  spdlog::info("[Proc {}] Number of Galois Threads: {}", node_id, nGaloisThreads);
}

template<typename VertexProperty>
void GraphAlgorithm<VertexProperty>::run(uint32_t &offload_mode, uint32_t &max_iterations)
{
  const char *OFFLOAD_DECISION_STR[] = {
    "NDP_OFFLOAD",
    "INC_OFFLOAD",
    "NO_OFFLOAD",
  };

  std::vector<uint32_t> idxTracker;
  std::vector<MPI_Request> bv_requests;
  std::vector<MPI_Request> data_requests;
  std::vector<MPI_Status> statuses;

  galois::DynamicBitSet unique_neighbors;
  size_t sbvSize = 0;
  size_t rbvSize = 0;

  double skewness = 0.0;

  if (node_type == COMPUTE_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_memory, 0);
    bv_requests = std::vector<MPI_Request>(num_memory);
    data_requests = std::vector<MPI_Request>(num_memory);
    statuses = std::vector<MPI_Status>(num_memory);

    for (int i = 0; i < num_memory; i++)
    {
      sbvSize += worker->bitCommVector_Send[i].size_bytes() * sizeof(uint64_t);
      rbvSize += worker->bitCommVector_Recv[i].size_bytes() * sizeof(uint64_t);
    }
  }
  else if (node_type == MEMORY_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_compute, 0);
    bv_requests = std::vector<MPI_Request>(num_compute);
    data_requests = std::vector<MPI_Request>(num_compute);
    statuses = std::vector<MPI_Status>(num_compute);

    unique_neighbors.resize(worker->num_vertices);
  }

  uint32_t completion = 0;
  OFFLOAD_DECISION memory_offload = NO_OFFLOAD;
  OFFLOAD_DECISION switch_offload = NO_OFFLOAD;
  OFFLOAD_DECISION init_decision = (offload_mode == 0) ? NDP_OFFLOAD : NO_OFFLOAD;
  uint32_t iteration = 0;
  size_t VertexProperty_size = sizeof(VertexProperty);

  size_t ndp_offload_threshold =
      (worker->num_vertices / 5) * (VertexProperty_size / sizeof(GNode)) * worker->distributed_graph->replication_factor;
  uint64_t inc_offload_threshold = worker->num_vertices / worker->distributed_graph->replication_factor;

  spdlog::info(
      "[Proc {}] NV:{}, RF:{}, NDP Offload Threshold: {}",
      worker->node_id,
      worker->num_vertices,
      worker->distributed_graph->replication_factor,
      ndp_offload_threshold);
  spdlog::info("[Proc {}] INC Offload Threshold: {}", worker->node_id, inc_offload_threshold);

  uint64_t frontier_size = 0;
  uint64_t neighbor_count = 0;
  uint64_t unique_neighbor_count = 0;
  uint64_t uNeighbors = 0;
  double offload_coeff = 0.0;
  double fetch_coeff = 0.0;
  double decision_coeff = 0.0;

  net.barrier();

  galois::StatTimer timer;
  galois::StatTimer traversal_timer;
  galois::StatTimer update_timer;
  galois::StatTimer cPhase1_timer;
  galois::StatTimer cPhase2_timer;

  std::vector<GNode> current_frontier;

  timer.start();

  uint64_t curr_bytes_moved = 0;
  uint64_t prev_bytes_moved = 0;

  while (worker_completion_count != num_compute && iteration < max_iterations)
  {
    memory_offload = init_decision;
    switch_offload = NO_OFFLOAD;
    ndp_decision = init_decision;
    inc_decision = NO_OFFLOAD;

    worker_completion_count = 0;
    completion = 0;
    uNeighbors = 0;
    neighbor_count = 0;

    if (node_type == COMPUTE_NODE)
    {
      current_frontier = this->frontier.getOffsets();

      if (offload_mode != 2)
      {
        memory_offload = NDPEngine(
            current_frontier,
            worker->coverage_vector,
            worker->out_degrees,
            ndp_offload_threshold,
            num_memory,
            num_compute,
            offload_coeff,
            fetch_coeff,
            decision_coeff,
            neighbor_count,
            frontier_size);
      }

      if (memory_offload == NDP_OFFLOAD)
      {
        switch_offload =
            INCEngine(current_frontier, worker->out_degrees, *worker->distributed_graph, inc_offload_threshold, num_memory);
      }
    }

    net.allReduce(&memory_offload, &ndp_decision, 1, MPI_UINT32_T, MPI_MIN);
    net.allReduce(&switch_offload, &inc_decision, 1, MPI_UINT32_T, MPI_MIN);

    // spdlog::info(
    //     "[Proc {}/{}] NDP Offload: {}, INC Offload: {}",
    //     worker->node_id,
    //     iteration,
    //     OFFLOAD_DECISION_STR[ndp_decision],
    //     OFFLOAD_DECISION_STR[inc_decision]);

    // Send the Frontier to all Traversers
    if (node_type == COMPUTE_NODE)
    {
      cPhase1_timer.start();

      if (ndp_decision == NDP_OFFLOAD)
      {
        generatePerThreadMatrix(current_frontier);

        for (int worker_id = 0; worker_id < num_memory; worker_id++)
        {
          // spdlog::info("[Proc {}/{}] Property Buffers: {}", this->worker->node_id, worker_id,
          // fmt_array(propertyBuffers[worker_id]));

          galois::do_all(
              galois::iterate(0ul, nGaloisThreads),
              [&](const size_t tid)
              {
                size_t offset = std::accumulate(
                    perThreadVCounts[worker_id].begin(), std::next(perThreadVCounts[worker_id].begin(), tid), 0);

                size_t &nVertices = perThreadVCounts[worker_id][tid];
                auto &offsetMatrix = perThreadOffsetMatrix[tid][worker_id];
                auto &transTable = worker->sTranslationTable[worker_id];
                auto &pBuffer = propertyBuffers[worker_id];

                for (uint64_t j = 0; j < nVertices; j++)
                {
                  GNode lid = offsetMatrix[j];
                  worker->bitCommVector_Send[worker_id].set(transTable[lid]);
                  pBuffer[offset + j] = vertex_properties[lid];

                  // spdlog::debug(
                  //     "[Proc {}/{}] Sending propertyBuffers: {}/{} to Memory Node: {}",
                  //     worker->node_id,
                  //     iteration,
                  //     worker->distributed_graph->getGlobalNode(lid),
                  //     vertex_properties[lid],
                  //     worker_id);
                }
              },
              galois::loopname("Generate Property Buffers"),
              galois::no_stats(),
              galois::steal());

          net.Isend(
              worker_id + num_compute,
              0,
              worker->bitCommVector_Send[worker_id].bitvec.data(),
              worker->bitCommVector_Send[worker_id].size_bytes(),
              MPI_UINT64_T,
              &bv_requests[worker_id]);

          net.Isend(
              worker_id + num_compute,
              0,
              propertyBuffers[worker_id].data(),
              std::accumulate(perThreadVCounts[worker_id].begin(), perThreadVCounts[worker_id].end(), 0),
              MPI_VERTEX_PROPERTY_T,
              &data_requests[worker_id]);
        }

        for (int i = 0; i < num_memory; i++)
        {
          MPI_Wait(&bv_requests[i], &statuses[i]);
          MPI_Wait(&data_requests[i], &statuses[i]);
          worker->bitCommVector_Send[i].reset();
        }
      }
      else
      {
        galois::do_all(
            galois::iterate(current_frontier.begin(), current_frontier.end()),
            [&](const GNode &lid)
            {
              uint32_t worker_id = worker->getVertexMemoryPartition(lid);
              worker->bitCommVector_Send[worker_id].set(worker->sTranslationTable[worker_id][lid]);
            },
            galois::loopname("Generate BitCommVector"),
            galois::no_stats(),
            galois::steal());

        for (int worker_id = 0; worker_id < num_memory; worker_id++)
        {
          // Only send BitSets to Memory Nodes
          net.Isend(
              worker_id + num_compute,
              0,
              worker->bitCommVector_Send[worker_id].bitvec.data(),
              worker->bitCommVector_Send[worker_id].size_bytes(),
              MPI_UINT64_T,
              &bv_requests[worker_id]);
        }

        for (int i = 0; i < num_memory; i++)
        {
          MPI_Wait(&bv_requests[i], &statuses[i]);
          worker->bitCommVector_Send[i].reset();
        }
      }
      cPhase1_timer.stop();
      idxTracker.assign(num_memory, 0);
    }
    else if (node_type == MEMORY_NODE)
    {
      cPhase1_timer.start();

      if (ndp_decision == NDP_OFFLOAD)
      {
        for (int i = 0; i < num_compute; i++)
        {
          net.Irecv(
              i,
              0,
              worker->bitCommVector_Recv[i].bitvec.data(),
              worker->bitCommVector_Recv[i].size_bytes(),
              MPI_UINT64_T,
              &statuses[i],
              &bv_requests[i]);

          net.Irecv(
              i,
              0,
              propertyBuffers[i].data(),
              propertyBuffers[i].size(),
              MPI_VERTEX_PROPERTY_T,
              &statuses[i],
              &data_requests[i]);

          // size_t bitCommVectorSize = worker->bitCommVector[i].size();
          // for (size_t j = 0; j < bitCommVectorSize; j++)
          // {
          //   if (worker->bitCommVector[i].test(j))
          //   {
          //     spdlog::info(
          //         "[Proc {}/{}] Updating GVertex/Trans/Property: {}/{}/{}",
          //         worker->node_id,
          //         iteration,
          //         worker->distributed_graph->getGlobalNode(worker->rTranslationTable[i][j]),
          //         j,
          //         propertyBuffers[i][idxTracker[i]]);

          //     GNode lid = worker->rTranslationTable[i][j];
          //     vertex_properties[lid].set(propertyBuffers[i][idxTracker[i]]);
          //     idxTracker[i]++;
          //   }
          // }

          const std::vector<GNode> &updated_property_vertices = worker->bitCommVector_Recv[i].getOffsets();
          const size_t &nVertices = updated_property_vertices.size();
          const size_t &uVerticesPerThread = 1 + ((nVertices > nGaloisThreads) ? nVertices / nGaloisThreads : 0);

          // spdlog::info("[Proc {}] Updated Property Vertices: {}", worker->node_id, fmt_array(updated_property_vertices));

          galois::do_all(
              galois::iterate(0ul, nGaloisThreads),
              [&](const size_t tid)
              {
                const size_t &start = tid * uVerticesPerThread;
                const size_t &end = (tid + 1) * uVerticesPerThread > nVertices ? nVertices : (tid + 1) * uVerticesPerThread;

                auto &transTable = worker->rTranslationTable[i];
                auto &pBuffer = propertyBuffers[i];

                for (size_t j = start; j < end; j++)
                {
                  GNode lid = transTable[updated_property_vertices[j]];
                  vertex_properties.set(lid, pBuffer[j]);
                }
              },
              galois::loopname("Update Property Vertices"),
              galois::no_stats(),
              galois::steal());
        }

        for (int i = 0; i < num_compute; i++)
        {
          worker->bitCommVector_Recv[i].reset();
        }
      }
      else
      {
        for (int i = 0; i < num_compute; i++)
        {
          net.Irecv(
              i,
              0,
              worker->bitCommVector_Recv[i].bitvec.data(),
              worker->bitCommVector_Recv[i].size_bytes(),
              MPI_UINT64_T,
              &statuses[i],
              &bv_requests[i]);

          const std::vector<GNode> &updated_property_vertices = worker->bitCommVector_Recv[i].getOffsets();
          const size_t &nVertices = updated_property_vertices.size();
          const size_t &uVerticesPerThread = 1 + ((nVertices > nGaloisThreads) ? nVertices / nGaloisThreads : 0);

          galois::do_all(
              galois::iterate(0ul, nGaloisThreads),
              [&](const size_t tid)
              {
                const size_t &start = tid * uVerticesPerThread;
                const size_t &end = (tid + 1) * uVerticesPerThread > nVertices ? nVertices : (tid + 1) * uVerticesPerThread;

                auto &transTable = worker->rTranslationTable[i];

                for (size_t j = start; j < end; j++)
                {
                  GNode lid = transTable[updated_property_vertices[j]];
                  vertex_properties.addUpdatedVertex(lid);
                }
              },
              galois::loopname("Update Property Vertices"),
              galois::no_stats(),
              galois::steal());
        }

        for (int i = 0; i < num_compute; i++)
        {
          worker->bitCommVector_Recv[i].reset();
        }
      }

      cPhase1_timer.stop();
      idxTracker.assign(num_compute, 0);
    }

    spdlog::debug("[Proc {}] Barrier 1: {}", worker->node_id, iteration);
    // net.barrier();

    if (node_type == MEMORY_NODE)
    {
      if (ndp_decision == NDP_OFFLOAD)
      {
        traversal_timer.start();
        worker->traverse(*this);
        traversal_timer.stop();

        // const galois::ThreadSafeOrderedSet<GNode> &updated_vertices = vertex_updates.getUpdatedVertices();
        const std::vector<GNode> updated_vertices = vertex_updates.getUpdatedVertices();
        spdlog::debug("[Proc {}] Updated Vertices: {}", this->worker->node_id, fmt_array(updated_vertices));

        if (inc_decision == INC_OFFLOAD)
        {
          std::vector<std::map<GNode, VertexProperty>> propertyMap(num_compute);
          for (const GNode &lid : updated_vertices)
          {
            uint32_t worker_id = worker->getVertexComputePartition(lid);
            worker->bitCommVector_Send[worker_id].set(worker->sAggrTranslationTable[worker_id][lid]);
            propertyMap[worker_id][worker->sAggrTranslationTable[worker_id][lid]] = vertex_updates[lid];
            // propertyBuffers[worker_id][idxTracker[worker_id]] = vertex_updates[lid];

            // spdlog::debug(
            //     "[Proc {}/{}] Sending GVertex/Update: {}/{} to Compute Node: {}",
            //     worker->node_id,
            //     iteration,
            //     gid,
            //     propertyBuffers[worker_id][idxTracker[worker_id]],
            //     worker_id);

            // idxTracker[worker_id]++;
          }

          for (uint32_t worker_id = 0; worker_id < num_compute; worker_id++)
          {
            for (auto elem : propertyMap[worker_id])
            {
              propertyBuffers[worker_id][idxTracker[worker_id]] = elem.second;
              idxTracker[worker_id]++;
            }

            net.Isend(
                worker_id,
                0,
                worker->bitCommVector_Send[worker_id].bitvec.data(),
                worker->bitCommVector_Send[worker_id].size_bytes(),
                MPI_UINT64_T,
                &bv_requests[worker_id]);

            net.Isend(
                worker_id,
                0,
                propertyBuffers[worker_id].data(),
                idxTracker[worker_id],
                MPI_VERTEX_PROPERTY_T,
                &data_requests[worker_id]);
          }
        }
        else
        {
          generatePerThreadMatrix(updated_vertices);

          galois::do_all(
              galois::iterate(0ul, nGaloisThreads),
              [&](const size_t tid)
              {
                for (int worker_id = 0; worker_id < num_compute; worker_id++)
                {
                  size_t offset = std::accumulate(
                      perThreadVCounts[worker_id].begin(), std::next(perThreadVCounts[worker_id].begin(), tid), 0);

                  size_t &nVertices = perThreadVCounts[worker_id][tid];
                  auto &offsetMatrix = perThreadOffsetMatrix[tid][worker_id];
                  auto &transTable = worker->sTranslationTable[worker_id];
                  auto &pBuffer = propertyBuffers[worker_id];

                  for (uint64_t j = 0; j < nVertices; j++)
                  {
                    GNode lid = offsetMatrix[j];
                    worker->bitCommVector_Send[worker_id].set(transTable[lid]);
                    pBuffer[offset + j] = vertex_updates[lid];

                    // spdlog::debug(
                    //     "[Proc {}/{}] Sending propertyBuffers: {}/{} to Compute Node: {}",
                    //     worker->node_id,
                    //     tid,
                    //     worker->distributed_graph->getGlobalNode(lid),
                    //     propertyBuffers[worker_id][j],
                    //     worker_id);
                  }
                }
              },
              galois::loopname("Generate Per Thread Matrix"),
              galois::no_stats(),
              galois::steal());

          for (int worker_id = 0; worker_id < num_compute; worker_id++)
          {
            // spdlog::info("[Proc {}/{}] Property Buffers: {}", this->worker->node_id, i, fmt_array(propertyBuffers[i]));
            net.Isend(
                worker_id,
                0,
                worker->bitCommVector_Send[worker_id].bitvec.data(),
                worker->bitCommVector_Send[worker_id].size_bytes(),
                MPI_UINT64_T,
                &bv_requests[worker_id]);

            net.Isend(
                worker_id,
                0,
                propertyBuffers[worker_id].data(),
                std::accumulate(perThreadVCounts[worker_id].begin(), perThreadVCounts[worker_id].end(), 0),
                MPI_VERTEX_PROPERTY_T,
                &data_requests[worker_id]);
          }
        }
      }
      else if (ndp_decision == NO_OFFLOAD)
      {
        // galois::ThreadSafeOrderedSet<GNode> &updated_vertices = vertex_properties.getUpdatedVertices();
        std::vector<GNode> updated_vertices = vertex_properties.getUpdatedVertices();
        uint64_t nVertices = updated_vertices.size();
        uint64_t uVerticesPerThread = 1 + ((nVertices > nGaloisThreads) ? nVertices / nGaloisThreads : 0);

        spdlog::debug("[Proc {}] Updated Vertices: {}", this->worker->node_id, fmt_array(updated_vertices));
        // std::vector<GNode> eBuffer;

        // for (const GNode &lid : updated_vertices)
        // {
        //   uint32_t worker_id = this->worker->getVertexComputePartition(lid);

        //   auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
        //   auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
        //   uint64_t num_edges = std::distance(ii, ei);

        //   eBuffer.resize(num_edges + 1);

        //   eBuffer[0] = this->worker->distributed_graph->getGlobalNode(lid);
        //   memcpy(eBuffer.data() + 1, &this->worker->distributed_graph->lgraph.edgeDst[*ii], num_edges * sizeof(GNode));

        //   galois::do_all(
        //       galois::iterate(uint64_t(1), num_edges + 1),
        //       [&](auto &edge) { eBuffer[edge] = this->worker->distributed_graph->getGlobalNode(eBuffer[edge]); },
        //       galois::loopname("Generate Updates"),
        //       galois::no_stats(),
        //       galois::steal());

        //   net.send(worker_id, 0, eBuffer.data(), num_edges + 1, MPI_GNODE_T);
        // }

        galois::do_all(
            galois::iterate(0ul, nGaloisThreads),
            [&](const size_t tid)
            {
              const size_t &start = tid * uVerticesPerThread;
              const size_t &end = (tid + 1) * uVerticesPerThread > nVertices ? nVertices : (tid + 1) * uVerticesPerThread;
              std::vector<std::vector<GNode>> eBuffer(2, std::vector<GNode>());
              std::vector<MPI_Request> buf_requests(2);
              std::vector<MPI_Status> buf_statuses(2);

              if (start >= end)
              {
                return;
              }

              uint32_t currBuf = start % 2;
              uint32_t prevBuf = 0;
              GNode &lid = updated_vertices[start];
              uint32_t worker_id = this->worker->getVertexComputePartition(lid);

              auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
              auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
              uint64_t num_edges = std::distance(ii, ei);

              eBuffer[currBuf].resize(num_edges + 1);
              eBuffer[currBuf][0] = this->worker->distributed_graph->getGlobalNode(lid);
              memcpy(
                  eBuffer[currBuf].data() + 1,
                  &this->worker->distributed_graph->lgraph.edgeDst[*ii],
                  num_edges * sizeof(GNode));

              for (size_t k = 1; k < num_edges + 1; k++)
              {
                unique_neighbors.set(eBuffer[currBuf][k]);
                eBuffer[currBuf][k] = this->worker->distributed_graph->getGlobalNode(eBuffer[currBuf][k]);
              }

              net.Isend(worker_id, 0, eBuffer[currBuf].data(), eBuffer[currBuf].size(), MPI_GNODE_T, &buf_requests[currBuf]);

              for (size_t j = start + 1; j < end; j++)
              {
                prevBuf = currBuf;
                currBuf = (prevBuf + 1) % 2;
                GNode &lid = updated_vertices[j];
                uint32_t worker_id = this->worker->getVertexComputePartition(lid);

                auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
                auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
                uint64_t num_edges = std::distance(ii, ei);

                eBuffer[currBuf].resize(num_edges + 1);
                eBuffer[currBuf][0] = this->worker->distributed_graph->getGlobalNode(lid);
                memcpy(
                    eBuffer[currBuf].data() + 1,
                    &this->worker->distributed_graph->lgraph.edgeDst[*ii],
                    num_edges * sizeof(GNode));

                for (size_t k = 1; k < num_edges + 1; k++)
                {
                  unique_neighbors.set(eBuffer[currBuf][k]);
                  eBuffer[currBuf][k] = this->worker->distributed_graph->getGlobalNode(eBuffer[currBuf][k]);
                }

                net.Isend(
                    worker_id, 0, eBuffer[currBuf].data(), eBuffer[currBuf].size(), MPI_GNODE_T, &buf_requests[currBuf]);
                MPI_Wait(&buf_requests[prevBuf], &buf_statuses[prevBuf]);
                eBuffer[prevBuf].clear();
              }

              MPI_Wait(&buf_requests[currBuf], &buf_statuses[currBuf]);
              eBuffer[currBuf].clear();
            },
            galois::loopname("Generate BitCommVector"),
            galois::no_stats(),
            galois::steal());

        // All Reduce Unique Neighbors
        unique_neighbor_count = unique_neighbors.count();
        net.allReduce(&unique_neighbor_count, &uNeighbors, 1, MPI_UINT64_T, MPI_SUM);
        unique_neighbors.reset();
      }
    }
    else if (node_type == COMPUTE_NODE)
    {
      cPhase2_timer.start();
      if (ndp_decision == NDP_OFFLOAD)
      {
        uint64_t bytes_recv = 0;
        auto update_worker = static_cast<UpdateWorker<VertexProperty> *>(worker);

        if (inc_decision == INC_OFFLOAD)
        {
          auto recv_data = update_worker->aggregator_worker.UpdateWorker_Recv_NDP_Offload();
          for (int i = 0; i < recv_data.size(); i++)
          {
            size_t bitCommVectorSize = recv_data[i].first.size();
            for (size_t j = 0; j < bitCommVectorSize; j++)
            {
              if (recv_data[i].first.test(j))
              {
                spdlog::debug(
                    "[Proc {}/{}] Aggregating GVertex/CurrProp/Update: {}/{}/{} from {}",
                    worker->node_id,
                    iteration,
                    worker->distributed_graph->getGlobalNode(j),
                    vertex_updates[j],
                    recv_data[i].second[idxTracker[i]],
                    i);
                GNode lid = j;
                worker->aggregate(*this, lid, recv_data[i].second[idxTracker[i]]);
                idxTracker[i]++;
              }
            }
          }

          // spdlog::info(
          //     "[Proc {}] Agg Updated Vertices: {}", this->worker->node_id, fmt_array(vertex_updates.getUpdatedVertices()));
        }
        else
        {
          for (int i = 0; i < num_memory; i++)
          {
            bytes_recv = net.Irecv(
                i + num_compute,
                0,
                worker->bitCommVector_Recv[i].bitvec.data(),
                worker->bitCommVector_Recv[i].size_bytes(),
                MPI_UINT64_T,
                &statuses[i],
                &bv_requests[i]);

            bytes_recv += net.Irecv(
                i + num_compute,
                0,
                propertyBuffers[i].data(),
                propertyBuffers[i].size(),
                MPI_VERTEX_PROPERTY_T,
                &statuses[i],
                &data_requests[i]);

            size_t bitCommVectorSize = worker->bitCommVector_Recv[i].size();
            const std::vector<GNode> updated_property_vertices = worker->bitCommVector_Recv[i].getOffsets();
            const size_t &nVertices = updated_property_vertices.size();
            const size_t &uVerticesPerThread = 1 + ((nVertices > nGaloisThreads) ? nVertices / nGaloisThreads : 0);

            uNeighbors += nVertices;

            galois::do_all(
                galois::iterate(0ul, nGaloisThreads),
                [&](const size_t tid)
                {
                  const size_t &start = tid * uVerticesPerThread;
                  const size_t &end =
                      (tid + 1) * uVerticesPerThread > nVertices ? nVertices : (tid + 1) * uVerticesPerThread;

                  auto &pBuffer = propertyBuffers[i];
                  auto &transTable = worker->rTranslationTable[i];

                  for (size_t j = start; j < end; j++)
                  {
                    GNode lid = transTable[updated_property_vertices[j]];
                    worker->aggregate(*this, lid, pBuffer[j]);
                  }
                },
                galois::loopname("Aggregate Property Vertices"),
                galois::no_stats(),
                galois::steal());

            // spdlog::info(
            //     "[Proc {}] Updated Vertices: {}", this->worker->node_id, fmt_array(vertex_updates.getUpdatedVertices()));
          }
        }
      }
      else if (ndp_decision == NO_OFFLOAD)
      {
        // TODO: Push this to init() and only do it once
        uint64_t max_out_degree = 0;
        std::vector<GNode> frontier_iter = frontier.getOffsets();
        for (const GNode lid : frontier_iter)
        {
          max_out_degree = std::max(max_out_degree, worker->out_degrees[lid]);
        }

        spdlog::debug("[Proc {}] Frontier Size: {}", worker->node_id, frontier_iter.size());

        // for (const GNode lid : frontier_iter)
        // {
        //   uint32_t worker_id = worker->getVertexMemoryPartition(lid);

        //   net.recv(worker_id + num_compute, 0, eBuffer.data(), max_out_degree + 1, MPI_GNODE_T, MPI_STATUS_IGNORE);

        //   // spdlog::debug(
        //   //     "[Proc {}/{}] Received Edges {} for Vertex {} from Memory Node: {}",
        //   //     worker->node_id,
        //   //     iteration,
        //   //     fmt_array(ebuffer),
        //   //     ebuffer[0],
        //   //     worker_id);

        //   GNode src = this->worker->distributed_graph->getLocalNode(eBuffer[0]);
        //   uint64_t num_edges = worker->out_degrees[src];

        //   VertexProperty src_prop = vertex_properties[src];

        //   galois::do_all(
        //       galois::iterate(uint64_t(1), num_edges + 1),
        //       [&](uint64_t idx)
        //       {
        //         GNode l_dst = worker->distributed_graph->getLocalNode(eBuffer[idx]);

        //         // spdlog::debug(
        //         //     "[Proc {}/{}] Aggregating Edge: {}/{} from Memory Node: {}",
        //         //     worker->node_id,
        //         //     iteration,
        //         //     src,
        //         //     l_dst,
        //         //     worker_id);

        //         // TODO: Need to Abstract this to the Graph Algorithm
        //         vertex_updates.addUpdate(l_dst, src_prop);
        //         // vertex_updates.minUpdate(l_dst, vertex_properties[src]);
        //       },
        //       galois::loopname("Generate Updates"),
        //       galois::no_stats(),
        //       galois::steal());

        //   eBuffer.clear();
        // }

        uint64_t nVertices = frontier_iter.size();
        uint64_t uVerticesPerThread = 1 + ((nVertices > nGaloisThreads) ? nVertices / nGaloisThreads : 0);

        galois::do_all(
            galois::iterate(0ul, nGaloisThreads),
            [&](const size_t tid)
            {
              const size_t &start = tid * uVerticesPerThread;
              const size_t &end = (tid + 1) * uVerticesPerThread > nVertices ? nVertices : (tid + 1) * uVerticesPerThread;
              std::vector<GNode> eBuffer(max_out_degree + 1);

              std::vector<MPI_Request> buf_requests(num_memory);
              std::vector<MPI_Status> buf_statuses(num_memory);

              for (size_t j = start; j < end; j++)
              {
                GNode &lid = frontier_iter[j];
                uint32_t worker_id = this->worker->getVertexMemoryPartition(lid);

                net.Irecv(
                    worker_id + num_compute,
                    0,
                    eBuffer.data(),
                    max_out_degree + 1,
                    MPI_GNODE_T,
                    &buf_statuses[worker_id],
                    &buf_requests[worker_id]);

                GNode src = this->worker->distributed_graph->getLocalNode(eBuffer[0]);

                for (size_t k = 1; k < worker->out_degrees[src] + 1; k++)
                {
                  GNode l_dst = worker->distributed_graph->getLocalNode(eBuffer[k]);
                  // vertex_updates.addUpdate(l_dst, vertex_properties[src]);
                  vertex_updates.minUpdate(l_dst, vertex_properties[src]);
                }
              }
            },
            galois::loopname("Generate BitCommVector"),
            galois::no_stats(),
            galois::steal());

        // All Reduce Unique Neighbors
        net.allReduce(&unique_neighbor_count, &uNeighbors, 1, MPI_UINT64_T, MPI_SUM);
      }
      cPhase2_timer.stop();
    }

    spdlog::debug("[Proc {}] Barrier 2: {}", worker->node_id, iteration);
    // net.barrier();

    if (node_type == COMPUTE_NODE)
    {
      this->frontier.reset();

      update_timer.start();
      worker->update(*this);
      update_timer.stop();

      if (termination_check())
      {
        completion = 1;
      }

      if (neighbor_count != 0)
      {
        uint64_t fetchDM = (neighbor_count + frontier_size) * sizeof(GNode) + sbvSize;
        uint64_t offloadDM = (uNeighbors + frontier_size) * VertexProperty_size + rbvSize + sbvSize;

        if (ndp_decision == NDP_OFFLOAD && fetchDM < offloadDM)
        {
          fetch_coeff = (offloadDM - fetchDM) / (double)offloadDM;
          offload_coeff = 0.0;
        }
        else if (ndp_decision == NO_OFFLOAD && fetchDM > offloadDM)
        {
          offload_coeff = (fetchDM - offloadDM) / (double)fetchDM;
          fetch_coeff = 0.0;
        }
        else
        {
          fetch_coeff = 0.0;
          offload_coeff = 0.0;
        }
      }

      for (uint32_t i = 0; i < num_memory; i++)
      {
        worker->bitCommVector_Send[i].reset();
        worker->bitCommVector_Recv[i].reset();
      }

      idxTracker.assign(num_memory, 0);
      vertex_updates.clear_uv();  // Clear the Updated Vertices ONLY
    }
    else if (node_type == MEMORY_NODE)
    {
      for (uint32_t i = 0; i < num_compute; i++)
      {
        MPI_Wait(&bv_requests[i], &statuses[i]);

        if (ndp_decision == NDP_OFFLOAD)
        {
          MPI_Wait(&data_requests[i], &statuses[i]);
        }

        worker->bitCommVector_Send[i].reset();
        worker->bitCommVector_Recv[i].reset();
      }

      idxTracker.assign(num_compute, 0);

      if (clear_updates)
      {
        vertex_updates.clear();
      }
      else
      {
        vertex_updates.clear_uv();
      }
    }

    vertex_properties.clear_uv();

    net.allReduce(&completion, &worker_completion_count, 1, MPI_UINT32_T, MPI_SUM);

    // spdlog::info("[Proc {}] Iteration: {}, WCC: {}", worker->node_id, iteration++, worker_completion_count);

    curr_bytes_moved = net.getBytesMoved();
    spdlog::info(
        "[Proc {}] Iteration: {}, Bytes Moved: {}", worker->node_id, iteration++, curr_bytes_moved - prev_bytes_moved);
    prev_bytes_moved = curr_bytes_moved;
  }

  timer.stop();

  uint64_t total_bytes = 0;
  uint64_t bytes = net.getBytesMoved();
  net.allReduce(&bytes, &total_bytes, 1, MPI_UINT64_T, MPI_SUM);

  if (worker->node_id == 0)
  {
    galois::runtime::reportStat_Single(algorithm_name, "Iterations", iteration);
    galois::runtime::reportStat_Single(algorithm_name, "TotalBytesMoved", total_bytes);
    galois::runtime::reportStat_Single(algorithm_name, "Timer_0", timer.get());
  }

  galois::runtime::reportStat_Single(algorithm_name, "TraversalTimer", traversal_timer.get());
  galois::runtime::reportStat_Single(algorithm_name, "UpdateTimer", update_timer.get());
  galois::runtime::reportStat_Single(algorithm_name, "CPhase1Timer", cPhase1_timer.get());
  galois::runtime::reportStat_Single(algorithm_name, "CPhase2Timer", cPhase2_timer.get());
}

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
    nGaloisThreads /= 2;
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
void GraphAlgorithm<VertexProperty>::run()
{
  std::vector<uint32_t> idxTracker;
  std::vector<MPI_Request> bv_requests;
  std::vector<MPI_Request> data_requests;
  std::vector<MPI_Status> statuses;

  if (node_type == COMPUTE_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_memory, 0);
    bv_requests = std::vector<MPI_Request>(num_memory);
    data_requests = std::vector<MPI_Request>(num_memory);
    statuses = std::vector<MPI_Status>(num_memory);
  }
  else if (node_type == MEMORY_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_compute, 0);
    bv_requests = std::vector<MPI_Request>(num_compute);
    data_requests = std::vector<MPI_Request>(num_compute);
    statuses = std::vector<MPI_Status>(num_compute);
  }

  uint32_t completion = 0;
  OFFLOAD_DECISION memory_offload = NO_OFFLOAD;
  OFFLOAD_DECISION switch_offload = NO_OFFLOAD;
  uint32_t iteration = 0;
  size_t ndp_offload_threshold = worker->total_vertices / 20;
  uint64_t inc_offload_threshold = std::accumulate(worker->out_degrees.begin(), worker->out_degrees.end(), 0) / 20;

  net.barrier();

  galois::StatTimer timer;
  galois::StatTimer traversal_timer;
  galois::StatTimer update_timer;
  galois::StatTimer cPhase1_timer;
  galois::StatTimer cPhase2_timer;

  timer.start();

  while (worker_completion_count != num_compute && iteration < 1000)
  {
    memory_offload = NDP_OFFLOAD;
    switch_offload = NO_OFFLOAD;
    ndp_decision = NDP_OFFLOAD;
    inc_decision = NO_OFFLOAD;

    worker_completion_count = 0;
    completion = 0;

    if (node_type == COMPUTE_NODE)
    {
      // memory_offload = NDPEngine(frontier, worker->coverage_vector, ndp_offload_threshold, *worker->distributed_graph,
      // num_memory);

      // switch_offload = INCEngine(frontier, worker->out_degrees, *worker->distributed_graph, inc_offload_threshold);
    }

    switch_offload = INC_OFFLOAD;

    // net.allReduce(&memory_offload, &ndp_decision, 1, MPI_UINT32_T, MPI_MIN);
    // net.allReduce(&switch_offload, &inc_decision, 1, MPI_UINT32_T, MPI_MIN);

    // Send the Frontier to all Traversers
    if (node_type == COMPUTE_NODE)
    {
      cPhase1_timer.start();
      const std::vector<GNode> &current_frontier = this->frontier.getOffsets();

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

      cPhase1_timer.stop();
      idxTracker.assign(num_memory, 0);
    }
    else if (node_type == MEMORY_NODE)
    {
      cPhase1_timer.start();
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

        if (switch_offload == INC_OFFLOAD)
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

        spdlog::debug("[Proc {}] Updated Vertices: {}", this->worker->node_id, fmt_array(updated_vertices));

        for (const GNode &lid : updated_vertices)
        {
          uint32_t worker_id = this->worker->getVertexComputePartition(lid);

          auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
          auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
          uint64_t num_edges = std::distance(ii, ei);

          std::vector<GNode> ebuffer(num_edges + 1);

          ebuffer[0] = this->worker->distributed_graph->getGlobalNode(lid);

          galois::do_all(
              galois::iterate(uint64_t(1), num_edges + 1),
              [&](auto &edge)
              {
                auto edge_idx = *ii + edge - 1;
                GNode dst = this->worker->distributed_graph->lgraph.getEdgeDst(edge_idx);

                // spdlog::info(
                //     "[Proc {}/{}] Sending Edge: {}/{} to Compute Node: {}",
                //     worker->node_id,
                //     iteration,
                //     idx,
                //     std::distance(ii, ei) + 1,
                //     worker_id);

                ebuffer[edge] = this->worker->distributed_graph->getGlobalNode(dst);
              },
              galois::loopname("Generate Updates"),
              galois::no_stats(),
              galois::steal());

          // spdlog::debug(
          //     "[Proc {}] Sending Edges {} for Vertex {} to Compute Node {}",
          //     this->worker->node_id,
          //     fmt_array(ebuffer),
          //     gid,
          //     worker_id);

          net.send(worker_id, 0, ebuffer.data(), ebuffer.size(), MPI_GNODE_T);
        }
      }
    }
    else if (node_type == COMPUTE_NODE)
    {
      cPhase2_timer.start();
      if (ndp_decision == NDP_OFFLOAD)
      {
        uint64_t bytes_recv = 0;
        auto update_worker = static_cast<UpdateWorker<VertexProperty> *>(worker);

        if (switch_offload == INC_OFFLOAD)
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

            // spdlog::info("[Proc {}] Updated Vertices: {}", this->worker->node_id,
            // fmt_array(updated_property_vertices));
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
        std::vector<GNode> ebuffer;
        ebuffer.resize(max_out_degree + 1, 0);

        spdlog::debug("[Proc {}] Frontier Size: {}", worker->node_id, frontier_iter.size());

        for (const GNode lid : frontier_iter)
        {
          uint32_t worker_id = worker->getVertexMemoryPartition(lid);

          net.recv(worker_id + num_compute, 0, ebuffer.data(), max_out_degree + 1, MPI_GNODE_T, MPI_STATUS_IGNORE);

          // spdlog::debug(
          //     "[Proc {}/{}] Received Edges {} for Vertex {} from Memory Node: {}",
          //     worker->node_id,
          //     iteration,
          //     fmt_array(ebuffer),
          //     ebuffer[0],
          //     worker_id);

          GNode src = this->worker->distributed_graph->getLocalNode(ebuffer[0]);
          uint64_t num_edges = worker->out_degrees[src];

          galois::do_all(
              galois::iterate(uint64_t(0), num_edges),
              [&](uint64_t idx)
              {
                GNode l_dst = worker->distributed_graph->getLocalNode(ebuffer[idx + 1]);

                // spdlog::debug(
                //     "[Proc {}/{}] Aggregating Edge: {}/{} from Memory Node: {}",
                //     worker->node_id,
                //     iteration,
                //     src,
                //     l_dst,
                //     worker_id);

                // TODO: Need to Abstract this to the Graph Algorithm
                vertex_updates.addUpdate(l_dst, vertex_properties[src]);
                // vertex_updates.minUpdate(l_dst, vertex_properties[src]);
              },
              galois::loopname("Generate Updates"),
              galois::no_stats(),
              galois::steal());

          ebuffer.clear();
        }
      }
      cPhase2_timer.stop();
    }

    spdlog::debug("[Proc {}] Barrier 2: {}", worker->node_id, iteration);
    // net.barrier();

    if (node_type == COMPUTE_NODE)
    {
      // this->frontier.clear();
      this->frontier.reset();

      update_timer.start();
      worker->update(*this);
      update_timer.stop();

      if (termination_check())
      {
        completion = 1;
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
        MPI_Wait(&data_requests[i], &statuses[i]);

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

    spdlog::info("[Proc {}] Iteration: {}, WCC: {}", worker->node_id, iteration++, worker_completion_count);
    // net.barrier();
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
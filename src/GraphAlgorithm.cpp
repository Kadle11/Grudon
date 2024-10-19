#include "GraphAlgorithm.hpp"

#include <algorithm>

#include "Logger.hpp"
#include "MPI.hpp"
#include "offload_engine/INCEngine.hpp"
#include "offload_engine/NDPEngine.hpp"

template<typename VertexProperty>
GraphAlgorithm<VertexProperty>::GraphAlgorithm(
    NODE_TYPE node_type,
    std::string algorithm_name,
    std::string graph_path,
    size_t num_compute,
    size_t num_memory,
    uint32_t node_id,
    MPICore &net)
    : algorithm_name(algorithm_name),
      node_type(node_type),
      net(net),
      num_compute(num_compute),
      num_memory(num_memory)
{
  if (node_type == COMPUTE_NODE)
  {
    worker = new UpdateWorker<VertexProperty>(graph_path, num_compute, num_memory, node_id, node_type, net);
    propertyBuffers.resize(num_memory);
    for (int i = 0; i < num_memory; i++)
    {
      propertyBuffers[i].allocateLocal(worker->sTranslationTable[i].size());
    }
  }
  else if (node_type == MEMORY_NODE)
  {
    worker = new TraverseWorker<VertexProperty>(graph_path, num_compute, num_memory, node_id, node_type, net);
    propertyBuffers.resize(num_compute);
    for (int i = 0; i < num_compute; i++)
    {
      propertyBuffers[i].allocateLocal(worker->sTranslationTable[i].size());
    }
  }

  clear_updates = false;

  worker_completion_count = 0;
  num_workers = num_compute + num_memory;

  frontier = galois::ThreadSafeOrderedSet<GNode>();
  vertex_properties.allocate(worker->num_vertices);
  vertex_updates.allocate(worker->num_vertices);
  MPI_VERTEX_PROPERTY_T = mpi_get_type<VertexProperty>();
}

template<typename VertexProperty>
void GraphAlgorithm<VertexProperty>::run()
{
  std::vector<uint32_t> idxTracker;

  if (node_type == COMPUTE_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_memory, 0);
  }
  else if (node_type == MEMORY_NODE)
  {
    idxTracker = std::vector<uint32_t>(num_compute, 0);
  }

  uint32_t completion = 0;
  OFFLOAD_DECISION memory_offload = NO_OFFLOAD;
  OFFLOAD_DECISION switch_offload = NO_OFFLOAD;
  uint32_t iteration = 0;
  size_t ndp_offload_threshold = worker->total_vertices / 20;
  uint64_t inc_offload_threshold = std::accumulate(worker->out_degrees.begin(), worker->out_degrees.end(), 0) / 20;

  while (worker_completion_count != num_compute && iteration < MAX_ITERATIONS)
  {
    memory_offload = NO_OFFLOAD;
    switch_offload = NO_OFFLOAD;
    ndp_decision = NO_OFFLOAD;
    inc_decision = NO_OFFLOAD;

    worker_completion_count = 0;
    completion = 0;

    if (node_type == COMPUTE_NODE)
    {
      memory_offload =
          NDPEngine(frontier, worker->coverage_vector, ndp_offload_threshold, *worker->distributed_graph, num_compute);

      // switch_offload = INCEngine(frontier, worker->out_degrees, *worker->distributed_graph, inc_offload_threshold);
    }

    net.allReduce(&memory_offload, &ndp_decision, 1, MPI_UINT32_T, MPI_MIN);
    // net.allReduce(&switch_offload, &inc_decision, 1, MPI_UINT32_T, MPI_MIN);

    // Send the Frontier to all Traversers
    if (node_type == COMPUTE_NODE)
    {
      for (const GNode &lid : frontier)
      {
        GNode gid = worker->distributed_graph->getGlobalNode(lid);
        uint32_t worker_id = worker->getVertexMemoryPartition(gid);
        worker->bitCommVector[worker_id].set(worker->sTranslationTable[worker_id][lid]);
        propertyBuffers[worker_id][idxTracker[worker_id]] = vertex_properties[lid];
        idxTracker[worker_id]++;

        spdlog::debug(
            "[Proc {}/{}] Sending GVertex/Trans/Property: {}/{}/{} to Memory Node: {}",
            worker->node_id,
            iteration,
            gid,
            worker->sTranslationTable[worker_id][lid],
            vertex_properties[lid],
            worker_id);
      }

      for (int i = 0; i < num_memory; i++)
      {
        spdlog::debug(
            "[Proc {}] Sending BitCommVector {} to Memory Node: {}",
            worker->node_id,
            fmt_array(worker->bitCommVector[i].bitvec),
            i);

        net.send(
            i + num_compute, 0, worker->bitCommVector[i].bitvec.data(), worker->bitCommVector[i].size_bytes(), MPI_UINT64_T);

        net.send(i + num_compute, 0, propertyBuffers[i].data(), idxTracker[i], MPI_VERTEX_PROPERTY_T);
        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_memory, 0);
    }
    else if (node_type == MEMORY_NODE)
    {
      for (int i = 0; i < num_compute; i++)
      {
        net.recv(
            i,
            0,
            worker->bitCommVector[i].bitvec.data(),
            worker->bitCommVector[i].size_bytes(),
            MPI_UINT64_T,
            MPI_STATUS_IGNORE);
        net.recv(i, 0, propertyBuffers[i].data(), propertyBuffers[i].size(), MPI_VERTEX_PROPERTY_T, MPI_STATUS_IGNORE);

        size_t bitCommVectorSize = worker->bitCommVector[i].size();

        spdlog::debug(
            "[Proc {}] Received BitCommVector {} from Compute Node: {}",
            worker->node_id,
            fmt_array(worker->bitCommVector[i].bitvec),
            i);

        for (size_t j = 0; j < bitCommVectorSize; j++)
        {
          if (worker->bitCommVector[i].test(j))
          {
            spdlog::debug(
                "[Proc {}/{}] Updating GVertex/Trans/Property: {}/{}/{}",
                worker->node_id,
                iteration,
                worker->distributed_graph->getGlobalNode(worker->rTranslationTable[i][j]),
                j,
                propertyBuffers[i][idxTracker[i]]);

            GNode lid = worker->rTranslationTable[i][j];
            vertex_properties.set(lid, propertyBuffers[i][idxTracker[i]]);
            idxTracker[i]++;
          }
        }

        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_compute, 0);
    }

    spdlog::debug("[Proc {}] Barrier 1: {}", worker->node_id, iteration);
    net.barrier();

    if (node_type == MEMORY_NODE)
    {
      if (ndp_decision == NDP_OFFLOAD)
      {
        worker->traverse(*this);

        const galois::ThreadSafeOrderedSet<GNode> &updated_vertices = vertex_updates.getUpdatedVertices();
        spdlog::debug("[Proc {}] Updated Vertices: {}", this->worker->node_id, fmt_array(updated_vertices));

        for (const GNode &lid : updated_vertices)
        {
          GNode gid = worker->distributed_graph->getGlobalNode(lid);
          uint32_t worker_id = worker->getVertexComputePartition(gid);
          worker->bitCommVector[worker_id].set(worker->sTranslationTable[worker_id][lid]);
          propertyBuffers[worker_id][idxTracker[worker_id]] = vertex_updates[lid];

          spdlog::debug(
              "[Proc {}/{}] Sending GVertex/Update: {}/{} to Compute Node: {}",
              worker->node_id,
              iteration,
              gid,
              propertyBuffers[worker_id][idxTracker[worker_id]],
              worker_id);

          idxTracker[worker_id]++;
        }

        for (int i = 0; i < num_compute; i++)
        {
          net.send(i, 0, worker->bitCommVector[i].bitvec.data(), worker->bitCommVector[i].size_bytes(), MPI_UINT64_T);
          net.send(i, 0, propertyBuffers[i].data(), idxTracker[i], MPI_VERTEX_PROPERTY_T);
        }
      }
      else if (ndp_decision == NO_OFFLOAD)
      {
        galois::ThreadSafeOrderedSet<GNode> &updated_vertices = vertex_properties.getUpdatedVertices();

        for (const GNode &lid : updated_vertices)
        {
          GNode gid = this->worker->distributed_graph->getGlobalNode(lid);
          uint32_t worker_id = this->worker->getVertexComputePartition(gid);

          auto ii = this->worker->distributed_graph->lgraph.edge_begin(lid);
          auto ei = this->worker->distributed_graph->lgraph.edge_end(lid);
          uint64_t num_edges = std::distance(ii, ei);

          std::vector<GNode> ebuffer(num_edges + 1);

          ebuffer[0] = gid;

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

          spdlog::debug(
              "[Proc {}] Sending Edges {} for Vertex {} to Compute Node {}",
              this->worker->node_id,
              fmt_array(ebuffer),
              gid,
              worker_id);
          net.send(worker_id, 0, ebuffer.data(), ebuffer.size(), MPI_GNODE_T);
        }
      }
    }
    else if (node_type == COMPUTE_NODE)
    {
      if (ndp_decision == NDP_OFFLOAD)
      {
        uint64_t bytes_recv = 0;

        for (int i = 0; i < num_memory; i++)
        {
          bytes_recv = net.recv(
              i + num_compute,
              0,
              worker->bitCommVector[i].bitvec.data(),
              worker->bitCommVector[i].size_bytes(),
              MPI_UINT64_T,
              MPI_STATUS_IGNORE);

          bytes_recv += net.recv(
              i + num_compute,
              0,
              propertyBuffers[i].data(),
              propertyBuffers[i].size(),
              MPI_VERTEX_PROPERTY_T,
              MPI_STATUS_IGNORE);

          size_t bitCommVectorSize = worker->bitCommVector[i].size();

          if (switch_offload == INC_OFFLOAD)
          {
            net.decrementBytesMoved(bytes_recv);
            bytes_recv = 0;

            galois::DynamicBitSet aggregate_bitset;
            aggregate_bitset.resize(worker->num_vertices);

            for (size_t j = 0; j < bitCommVectorSize; j++)
            {
              if (worker->bitCommVector[i].test(j))
              {
                spdlog::debug(
                    "[Proc {}/{}] Aggregating GVertex/CurrProp/Update: {}/{}/{} from {}",
                    worker->node_id,
                    iteration,
                    worker->distributed_graph->getGlobalNode(worker->rTranslationTable[i][j]),
                    vertex_updates[worker->rTranslationTable[i][j]],
                    propertyBuffers[i][idxTracker[i]],
                    i);

                GNode lid = worker->rTranslationTable[i][j];
                worker->aggregate(*this, lid, propertyBuffers[i][idxTracker[i]]);
                aggregate_bitset.set(lid);
                idxTracker[i]++;
              }
            }

            bytes_recv =
                aggregate_bitset.size_bytes() * sizeof(uint64_t) + aggregate_bitset.count() * sizeof(VertexProperty);
            net.incrementBytesMoved(bytes_recv);
            aggregate_bitset.reset();
            bytes_recv = 0;
          }
          else
          {
            for (size_t j = 0; j < bitCommVectorSize; j++)
            {
              if (worker->bitCommVector[i].test(j))
              {
                spdlog::debug(
                    "[Proc {}/{}] Aggregating GVertex/CurrProp/Update: {}/{}/{} from {}",
                    worker->node_id,
                    iteration,
                    worker->distributed_graph->getGlobalNode(worker->rTranslationTable[i][j]),
                    vertex_updates[worker->rTranslationTable[i][j]],
                    propertyBuffers[i][idxTracker[i]],
                    i);

                GNode lid = worker->rTranslationTable[i][j];
                worker->aggregate(*this, lid, propertyBuffers[i][idxTracker[i]]);
                idxTracker[i]++;
              }
            }
          }
        }
      }
      else if (ndp_decision == NO_OFFLOAD)
      {
        // Recive the Edges for the frontier from the Memory Nodes
        uint64_t max_out_degree = 0;
        for (const GNode &lid : frontier)
        {
          max_out_degree = std::max(max_out_degree, worker->out_degrees[lid]);
        }

        std::vector<GNode> ebuffer;
        ebuffer.resize(max_out_degree + 1, 0);

        for (const GNode &lid : frontier)
        {
          GNode gid = worker->distributed_graph->getGlobalNode(lid);
          uint32_t worker_id = worker->getVertexMemoryPartition(gid);

          net.recv(worker_id + num_compute, 0, ebuffer.data(), max_out_degree + 1, MPI_GNODE_T, MPI_STATUS_IGNORE);

          spdlog::debug(
              "[Proc {}/{}] Received Edges {} for Vertex {} from Memory Node: {}",
              worker->node_id,
              iteration,
              fmt_array(ebuffer),
              ebuffer[0],
              worker_id);

          GNode src = this->worker->distributed_graph->getLocalNode(ebuffer[0]);
          uint64_t num_edges = worker->out_degrees[src];

          galois::do_all(
              galois::iterate(uint64_t(0), num_edges),
              [&](uint64_t idx)
              {
                GNode l_dst = worker->distributed_graph->getLocalNode(ebuffer[idx + 1]);

                spdlog::debug(
                    "[Proc {}/{}] Aggregating Edge: {}/{} from Memory Node: {}",
                    worker->node_id,
                    iteration,
                    src,
                    l_dst,
                    worker_id);

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
    }

    spdlog::debug("[Proc {}] Barrier 2: {}", worker->node_id, iteration);
    net.barrier();

    if (node_type == COMPUTE_NODE)
    {
      this->frontier.clear();

      worker->update(*this);

      if (termination_check())
      {
        completion = 1;
      }

      for (uint32_t i = 0; i < num_memory; i++)
      {
        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_memory, 0);
      vertex_updates.clear_uv();  // Clear the Updated Vertices ONLY
    }
    else if (node_type == MEMORY_NODE)
    {
      for (uint32_t i = 0; i < num_compute; i++)
      {
        worker->bitCommVector[i].reset();
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
    net.barrier();
  }

  uint64_t total_bytes = 0;
  uint64_t bytes = net.getBytesMoved();
  net.allReduce(&bytes, &total_bytes, 1, MPI_UINT64_T, MPI_SUM);

  if (worker->node_id == 0)
  {
    galois::runtime::reportStat_Single(algorithm_name, "Iterations", iteration);
    galois::runtime::reportStat_Single(algorithm_name, "TotalBytesMoved", total_bytes);
  }
}
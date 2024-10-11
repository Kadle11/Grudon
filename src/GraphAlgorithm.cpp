#include "GraphAlgorithm.hpp"

#include "Logger.hpp"
#include "MPI.hpp"

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

  worker_completion_count = 0;
  num_workers = num_compute + num_memory;

  frontier.reserve(worker->num_vertices);
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
  uint32_t iteration = 0;

  while (worker_completion_count != num_compute && iteration < MAX_ITERATIONS)
  {
    worker_completion_count = 0;
    completion = 0;

    // Send the Frontier to all Traversers
    if (node_type == COMPUTE_NODE)
    {
      std::sort(frontier.begin(), frontier.end());
      for (GNode &lid : frontier)
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

      this->frontier.clear();
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
      worker->traverse(*this);

      const std::set<GNode> &updated_vertices = vertex_updates.getUpdatedVertices();
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
    else if (node_type == COMPUTE_NODE)
    {
      for (int i = 0; i < num_memory; i++)
      {
        net.recv(
            i + num_compute,
            0,
            worker->bitCommVector[i].bitvec.data(),
            worker->bitCommVector[i].size_bytes(),
            MPI_UINT64_T,
            MPI_STATUS_IGNORE);

        net.recv(
            i + num_compute,
            0,
            propertyBuffers[i].data(),
            propertyBuffers[i].size(),
            MPI_VERTEX_PROPERTY_T,
            MPI_STATUS_IGNORE);

        size_t bitCommVectorSize = worker->bitCommVector[i].size();
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

    // TODO: Clear the BitCommVector, UpdatedVertices and IdxTracker
    spdlog::debug("[Proc {}] Barrier 2: {}", worker->node_id, iteration);
    net.barrier();

    if (node_type == COMPUTE_NODE)
    {
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
      vertex_updates.clear_uv(); // Clear the Updated Vertices ONLY
    }
    else if (node_type == MEMORY_NODE)
    {
      for (uint32_t i = 0; i < num_compute; i++)
      {
        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_compute, 0);
      vertex_updates.clear();
    }

    vertex_properties.clear_uv();

    net.allReduce(&completion, &worker_completion_count, 1, MPI_UINT32_T, MPI_SUM);

    spdlog::info("[Proc {}] Iteration: {}, WCC: {}", worker->node_id, iteration++, worker_completion_count);
    net.barrier();
  }
}
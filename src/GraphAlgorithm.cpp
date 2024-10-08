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
  }
  else if (node_type == MEMORY_NODE)
  {
    worker = new TraverseWorker<VertexProperty>(graph_path, num_compute, num_memory, node_id, node_type, net);
  }

  worker_completion_count = 0;
  num_workers = num_compute + num_memory;

  propertyBuffers.resize(num_workers);
  for (int i = 0; i < num_workers; i++)
  {
    propertyBuffers[i].allocateLocal(worker->addrTranslationTable[i].size());
  }

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

    // Send the Frontier to all Traversers
    if (node_type == COMPUTE_NODE)
    {
      std::sort(frontier.begin(), frontier.end());
      for (GNode &lid : frontier)
      {
        uint32_t worker_id = worker->getVertexMemoryPartition(lid);
        worker->bitCommVector[worker_id].set(lid);
        propertyBuffers[worker_id][idxTracker[worker_id]] = vertex_properties[lid];
        idxTracker[worker_id]++;

        spdlog::debug(
            "[Proc {}] Sending Vertex/Property: {}/{} to Memory Node: {}",
            worker->node_id,
            lid,
            vertex_properties[lid],
            worker_id);
      }

      for (int i = 0; i < num_memory; i++)
      {
        net.send(
            i + num_memory, 0, worker->bitCommVector[i].bitvec.data(), worker->bitCommVector[i].size_bytes(), MPI_UINT64_T);

        net.send(i + num_memory, 0, propertyBuffers[i].data(), idxTracker[i], MPI_VERTEX_PROPERTY_T);
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

        for (size_t j = 0; j < bitCommVectorSize; j++)
        {
          if (worker->bitCommVector[i].test(j))
          {
            spdlog::debug(
                "[Proc {}] Updating Vertex/Property: {}/{}",
                worker->node_id,
                worker->addrTranslationTable[i][j],
                propertyBuffers[i][idxTracker[i]]);
            GNode lid = worker->addrTranslationTable[i][j];
            vertex_properties.set(lid, propertyBuffers[i][idxTracker[i]]);
            idxTracker[i]++;
          }
        }

        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_compute, 0);
    }

    // TODO: Clear the BitCommVector, UpdatedVertices and IdxTracker
    net.barrier();

    if (node_type == MEMORY_NODE)
    {
      worker->traverse(*this);

      const std::set<GNode> &updated_vertices = vertex_updates.getUpdatedVertices();
      for (const GNode &lid : updated_vertices)
      {
        uint32_t worker_id = worker->getVertexComputePartition(lid);
        worker->bitCommVector[worker_id].set(lid);
        propertyBuffers[worker_id][idxTracker[worker_id]] = vertex_updates[lid];

        spdlog::debug(
            "[Proc {}] Sending Vertex/Update: {}/{} to Compute Node: {}",
            worker->node_id,
            lid,
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
            i + num_memory,
            0,
            worker->bitCommVector[i].bitvec.data(),
            worker->bitCommVector[i].size_bytes(),
            MPI_UINT64_T,
            MPI_STATUS_IGNORE);

        net.recv(
            i + num_memory,
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
                "[Proc {}] Aggregating Vertex/CurrProp/Update: {}/{}/{}",
                worker->node_id,
                worker->addrTranslationTable[i][j],
                vertex_updates[worker->addrTranslationTable[i][j]],
                propertyBuffers[i][idxTracker[i]]);

            GNode lid = worker->addrTranslationTable[i][j];
            worker->aggregate(*this, lid, propertyBuffers[i][idxTracker[i]]);
            idxTracker[i]++;
          }
        }
      }
    }

    // TODO: Clear the BitCommVector, UpdatedVertices and IdxTracker
    net.barrier();

    if (node_type == COMPUTE_NODE)
    {
      worker->update(*this);
      if (termination_check())
      {
        completion = 1;
      }

      for (uint32_t i = 0; i < num_compute; i++)
      {
        worker->bitCommVector[i].reset();
      }

      idxTracker.assign(num_memory, 0);
      vertex_updates.clear_uv();
    }
    else if (node_type == MEMORY_NODE)
    {
      for (uint32_t i = 0; i < num_memory; i++)
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
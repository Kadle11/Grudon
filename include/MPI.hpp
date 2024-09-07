#ifndef MPICORE_HPP
#define MPICORE_HPP

#include <mpi.h>

#define MPI_GNODE_T MPI_UINT32_T

class MPICore
{
 public:
  MPICore(int argc, char** argv)
  {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided != MPI_THREAD_MULTIPLE)
    {
      throw std::runtime_error("MPI_THREAD_MULTIPLE not supported");
    }

    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  }

  ~MPICore()
  {
    MPI_Finalize();
  }

  int getRank()
  {
    return rank;
  }

  int getNumProcs()
  {
    return num_procs;
  }

  int send(int dest, int tag, void* buf, int count, MPI_Datatype datatype)
  {
    return MPI_Send(buf, count, datatype, dest, tag, MPI_COMM_WORLD);
  }

  int recv(int source, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Status* status)
  {
    return MPI_Recv(buf, count, datatype, source, tag, MPI_COMM_WORLD, status);
  }

  void barrier()
  {
    MPI_Barrier(MPI_COMM_WORLD);
  }

  void selectiveBarrier(int color)
  {
    MPI_Comm comm;
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &comm);
    MPI_Barrier(comm);
  }

  void isend(int dest, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Request* request)
  {
    MPI_Isend(buf, count, datatype, dest, tag, MPI_COMM_WORLD, request);
  }

  void irecv(int source, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Request* request)
  {
    MPI_Irecv(buf, count, datatype, source, tag, MPI_COMM_WORLD, request);
  }

 private:
  int rank;
  int num_procs;
};

#endif  // MPICORE_HPP
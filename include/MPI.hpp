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
    bytes_moved = 0;
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
    MPI_Type_size(datatype, &stype_size);
    bytes_moved += count * stype_size;
    return MPI_Send(buf, count, datatype, dest, tag, MPI_COMM_WORLD);
  }

  uint64_t recv(int source, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Status* status)
  {
    MPI_Recv(buf, count, datatype, source, tag, MPI_COMM_WORLD, &local_status);
    MPI_Get_count(&local_status, datatype, &local_count);
    MPI_Type_size(datatype, &rtype_size);
    bytes_moved += local_count * rtype_size;
    return local_count * rtype_size;
  }

  int Isend(int dest, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Request* request = NULL)
  {
    MPI_Type_size(datatype, &stype_size);
    bytes_moved += count * stype_size;
    return MPI_Isend(buf, count, datatype, dest, tag, MPI_COMM_WORLD, request);
  }

  uint64_t
  Irecv(int source, int tag, void* buf, int count, MPI_Datatype datatype, MPI_Status* status, MPI_Request* request = NULL)
  {
    MPI_Irecv(buf, count, datatype, source, tag, MPI_COMM_WORLD, request);
    MPI_Wait(request, status);
    MPI_Get_count(status, datatype, &local_count);
    MPI_Type_size(datatype, &rtype_size);
    bytes_moved += local_count * rtype_size;
    return local_count * rtype_size;
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

  void broadcast(void* buf, int count, MPI_Datatype datatype, int root)
  {
    MPI_Bcast(buf, count, datatype, root, MPI_COMM_WORLD);
  }

  void reduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root)
  {
    MPI_Reduce(sendbuf, recvbuf, count, datatype, op, root, MPI_COMM_WORLD);
  }

  void allReduce(void* sendbuf, void* recvbuf, int count, MPI_Datatype datatype, MPI_Op op)
  {
    MPI_Allreduce(sendbuf, recvbuf, count, datatype, op, MPI_COMM_WORLD);
  }

  uint64_t& getBytesMoved()
  {
    return bytes_moved;
  }

  void decrementBytesMoved(const uint64_t& bytes)
  {
    bytes_moved -= bytes;
  }

  void incrementBytesMoved(const uint64_t& bytes)
  {
    bytes_moved += bytes;
  }

 private:
  int rank;
  int num_procs;
  uint64_t bytes_moved;
  int local_count;
  int stype_size;
  int rtype_size;
  MPI_Status local_status;
};

template<typename T>
[[nodiscard]] constexpr MPI_Datatype mpi_get_type() noexcept
{
  MPI_Datatype mpi_type = MPI_DATATYPE_NULL;

  if constexpr (std::is_same<T, char>::value)
  {
    mpi_type = MPI_CHAR;
  }
  else if constexpr (std::is_same<T, signed char>::value)
  {
    mpi_type = MPI_SIGNED_CHAR;
  }
  else if constexpr (std::is_same<T, unsigned char>::value)
  {
    mpi_type = MPI_UNSIGNED_CHAR;
  }
  else if constexpr (std::is_same<T, wchar_t>::value)
  {
    mpi_type = MPI_WCHAR;
  }
  else if constexpr (std::is_same<T, signed short>::value)
  {
    mpi_type = MPI_SHORT;
  }
  else if constexpr (std::is_same<T, unsigned short>::value)
  {
    mpi_type = MPI_UNSIGNED_SHORT;
  }
  else if constexpr (std::is_same<T, signed int>::value)
  {
    mpi_type = MPI_INT;
  }
  else if constexpr (std::is_same<T, unsigned int>::value)
  {
    mpi_type = MPI_UNSIGNED;
  }
  else if constexpr (std::is_same<T, signed long int>::value)
  {
    mpi_type = MPI_LONG;
  }
  else if constexpr (std::is_same<T, unsigned long int>::value)
  {
    mpi_type = MPI_UNSIGNED_LONG;
  }
  else if constexpr (std::is_same<T, signed long long int>::value)
  {
    mpi_type = MPI_LONG_LONG;
  }
  else if constexpr (std::is_same<T, unsigned long long int>::value)
  {
    mpi_type = MPI_UNSIGNED_LONG_LONG;
  }
  else if constexpr (std::is_same<T, float>::value)
  {
    mpi_type = MPI_FLOAT;
  }
  else if constexpr (std::is_same<T, double>::value)
  {
    mpi_type = MPI_DOUBLE;
  }
  else if constexpr (std::is_same<T, long double>::value)
  {
    mpi_type = MPI_LONG_DOUBLE;
  }
  else if constexpr (std::is_same<T, std::int8_t>::value)
  {
    mpi_type = MPI_INT8_T;
  }
  else if constexpr (std::is_same<T, std::int16_t>::value)
  {
    mpi_type = MPI_INT16_T;
  }
  else if constexpr (std::is_same<T, std::int32_t>::value)
  {
    mpi_type = MPI_INT32_T;
  }
  else if constexpr (std::is_same<T, std::int64_t>::value)
  {
    mpi_type = MPI_INT64_T;
  }
  else if constexpr (std::is_same<T, std::uint8_t>::value)
  {
    mpi_type = MPI_UINT8_T;
  }
  else if constexpr (std::is_same<T, std::uint16_t>::value)
  {
    mpi_type = MPI_UINT16_T;
  }
  else if constexpr (std::is_same<T, std::uint32_t>::value)
  {
    mpi_type = MPI_UINT32_T;
  }
  else if constexpr (std::is_same<T, std::uint64_t>::value)
  {
    mpi_type = MPI_UINT64_T;
  }
  else if constexpr (std::is_same<T, bool>::value)
  {
    mpi_type = MPI_C_BOOL;
  }

  assert(mpi_type != MPI_DATATYPE_NULL);
  return mpi_type;
}

#endif  // MPICORE_HPP

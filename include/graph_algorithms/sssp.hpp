#ifndef SSSP_HPP
#define SSSP_HPP

#include "GraphAlgorithm.hpp"

template<typename VertexProperty>
class SSSP : public GraphAlgorithm<VertexProperty>
{
 public:
  SSSP(
      NODE_TYPE node_type,
      std::string algorithm_name,
      std::string graph_path,
      size_t num_compute,
      size_t num_memory,
      uint32_t node_id,
      MPICore &net,
      std::string partitioning_scheme_file = "");

  void init() override;
  inline void apply_updates() override;
  inline void gen_updates() override;
  inline void update_frontier() override;
  inline void aggregate(GNode &lid, const VertexProperty &buffer_val) override;
  inline bool termination_check() override;
  void printState() override;
  void verify() override;

 private:
};

// Explicit Instantiation
template class SSSP<uint64_t>;
template class SSSP<uint32_t>;

#endif  // SSSP_HPP - 737266696
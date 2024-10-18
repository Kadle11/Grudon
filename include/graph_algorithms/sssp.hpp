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
      MPICore &net);

  void init() override;
  void apply_updates() override;
  void gen_updates() override;
  void update_frontier() override;
  void aggregate(GNode &lid, const VertexProperty &buffer_val) override;
  bool termination_check() override;
  void printState() override;

 private:
};

// Explicit Instantiation
template class SSSP<uint64_t>;
// template class SSSP<uint32_t>;

#endif  // SSSP_HPP
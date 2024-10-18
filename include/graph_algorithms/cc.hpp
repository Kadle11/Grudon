#ifndef CC_HPP
#define CC_HPP

#include "GraphAlgorithm.hpp"

template<typename VertexProperty>
class CC : public GraphAlgorithm<VertexProperty>
{
 public:
  CC(
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
template class CC<uint64_t>;
// template class CC<uint32_t>;

#endif  // CC_HPP
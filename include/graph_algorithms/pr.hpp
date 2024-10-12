#ifndef PR_HPP
#define PR_HPP

#include "GraphAlgorithm.hpp"

#define DAMPING_FACTOR 0.85
#define TOLERANCE      1e-3

template<typename VertexProperty>
class PageRank : public GraphAlgorithm<VertexProperty>
{
 public:
  PageRank(
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
  PropertyList<VertexProperty> pr_vals;
  PropertyList<VertexProperty> prev_updates;
};

// Explicit Instantiation
template class PageRank<float>;
template class PageRank<double>;

#endif  // PR_HPP
#ifndef PR_HPP
#define PR_HPP

#include "GraphAlgorithm.hpp"

#define DAMPING_FACTOR 0.85
#define TOLERANCE      1e-6

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
  PropertyList<VertexProperty> pr_vals;
  PropertyList<VertexProperty> prev_updates;
};

// Explicit Instantiation
template class PageRank<float>;
template class PageRank<double>;

#endif  // PR_HPP - 7261630600
#ifndef BC_HPP
#define BC_HPP

#include "GraphAlgorithm.hpp"

template<typename VertexProperty>
class BC : public GraphAlgorithm<VertexProperty>
{
 public:
  BC(NODE_TYPE node_type,
     std::string_view algorithm_name,
     std::string_view graph_path,
     std::size_t num_compute,
     std::size_t num_memory,
     unsigned int node_id,
     MPICore& net,
     std::string_view partitioning_scheme_file = "");

  void init() override;
  void apply_updates() override;
  void gen_updates() override;
  void update_frontier() override;
  void aggregate(GNode& lid, const VertexProperty& buffer_val) override;
  bool termination_check() override;
  void printState() override;
  void verify() override;

 private:
};

template class BC<unsigned long>;
template class BC<unsigned int>;

#endif

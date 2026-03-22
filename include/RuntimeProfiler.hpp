#ifndef RUNTIME_PROFILER_HPP
#define RUNTIME_PROFILER_HPP

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>

#include "Graph.hpp"

#if defined(GRUDON_ENABLE_PERF_CPP)
#include <perfcpp/counter_definition.h>
#include <perfcpp/event_counter.h>
#include <perfcpp/sampler.h>
#endif

class RuntimeProfiler
{
 public:
  RuntimeProfiler(const std::string& algorithm_name, uint32_t rank);

  static const std::array<const char*, 8>& baseOperationNames();
  [[nodiscard]] const std::vector<std::string>& operationNames() const;
  [[nodiscard]] bool hasOperation(const std::string& operation) const;

  void startIteration();
  void stopIteration();
  void writeTrace();

  void increment(const std::string& operation);
  [[nodiscard]] bool startOperation(const std::string& operation);
  void stopOperation(const std::string& operation, bool was_started);

  void addHostToRemoteBytes(uint64_t bytes);
  void addRemoteToHostBytes(uint64_t bytes);

  [[nodiscard]] uint64_t hostToRemoteBytes() const;
  [[nodiscard]] uint64_t remoteToHostBytes() const;
  [[nodiscard]] uint64_t callCount(const std::string& op_name) const;
  [[nodiscard]] uint64_t callGraphSamples() const;
  [[nodiscard]] uint64_t callGraphFrames() const;
  [[nodiscard]] bool isCallGraphEnabled() const;
  [[nodiscard]] uint16_t callGraphMaxDepth() const;

  void writeJson(
      uint32_t world_size,
      NODE_TYPE node_type,
      uint64_t iterations,
      uint64_t global_host_to_remote,
      uint64_t global_remote_to_host,
      const std::unordered_map<std::string, uint64_t>& global_calls,
      uint64_t global_callgraph_samples,
      uint64_t global_callgraph_frames) const;

 private:
#if defined(GRUDON_ENABLE_PERF_CPP)
  void tryAddEvent(perf::EventCounter& counter, const std::string& event_name);
  void initPerfCpp();
  void updateCallGraphStats();
#endif

  std::string algorithm_name_;
  uint32_t rank_;
  bool enabled_{false};
  bool pr_internal_enabled_{false};
  bool pr_fine_enabled_{false};
  std::string output_dir_;
  std::string output_prefix_;
  std::vector<std::string> operation_names_;
  uint64_t host_to_remote_bytes_{0};
  uint64_t remote_to_host_bytes_{0};
  uint64_t callgraph_samples_{0};
  uint64_t callgraph_frames_{0};
  bool callgraph_enabled_{false};
  uint16_t callgraph_max_depth_{0};

  std::unordered_map<std::string, uint64_t> call_counts_;
  std::unordered_map<std::string, std::unordered_map<std::string, double>> operation_counters_;
  std::vector<std::string> enabled_events_;
  std::vector<std::string> unsupported_events_;

#if defined(GRUDON_ENABLE_PERF_CPP)
  std::unique_ptr<perf::CounterDefinition> counter_definition_;
  std::unordered_map<std::string, perf::EventCounter> operation_perf_counters_;
  perf::Sampler sampler_;
  bool sampler_ready_{false};
  bool sampler_running_{false};
#endif
};

class ScopedOperationProfile
{
 public:
  ScopedOperationProfile(RuntimeProfiler& profiler, std::string operation_name)
      : profiler_(profiler), operation_name_(std::move(operation_name))
  {
    started_ = profiler_.startOperation(operation_name_);
  }

  ~ScopedOperationProfile()
  {
    profiler_.stopOperation(operation_name_, started_);
  }

 private:
  RuntimeProfiler& profiler_;
  std::string operation_name_;
  bool started_{false};
};

#endif  // RUNTIME_PROFILER_HPP

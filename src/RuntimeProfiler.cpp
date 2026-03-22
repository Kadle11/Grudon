#include "RuntimeProfiler.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

#if defined(GRUDON_ENABLE_PERF_CPP)
#include <perfcpp/hardware_info.h>
#endif

namespace {

constexpr std::array<const char*, 8> kOperationNames = {
    "iteration",
    "host_prepare_frontier",
    "host_send_updates_to_remote",
    "remote_apply_host_updates",
    "remote_generate_updates",
    "remote_send_updates_to_host",
    "host_receive_remote_updates",
    "host_update_frontier"};

constexpr std::array<const char*, 9> kPageRankInternalOperationNames = {
    "pr_apply_collect_frontier",
    "pr_apply_filter_updates",
    "pr_apply_commit_updates",
    "pr_gen_collect_sources",
    "pr_gen_collect_edge_contribs",
    "pr_gen_scatter_updates",
    "pr_frontier_collect_candidates",
    "pr_frontier_select_active",
    "pr_frontier_commit_active"};

constexpr std::array<const char*, 14> kPageRankFineOperationNames = {
    "pr_apply_collect_frontier",
    "pr_apply_filter_updates",
    "pr_apply_load_active_updates",
    "pr_apply_accumulate_pr",
    "pr_apply_recompute_property",
    "pr_apply_clear_buffers",
    "pr_gen_collect_sources",
    "pr_gen_count_edges",
    "pr_gen_expand_edge_contribs",
    "pr_gen_scatter_updates",
    "pr_frontier_collect_candidates",
    "pr_frontier_select_active",
    "pr_frontier_set_bits",
    "pr_frontier_store_prev_updates"};

[[nodiscard]] std::string normalizeAsciiLower(std::string value)
{
  for (char& ch : value)
  {
    if (ch >= 'A' && ch <= 'Z')
    {
      ch = static_cast<char>(ch - 'A' + 'a');
    }
  }

  return value;
}

[[nodiscard]] bool hasPrefix(const std::string& value, const char* prefix)
{
  const std::string prefix_str(prefix);
  return value.rfind(prefix_str, 0) == 0;
}

[[nodiscard]] bool shouldEnablePhaseOperation(const std::string& op_name, const std::string& selected_phase)
{
  if (selected_phase.empty() || selected_phase == "all")
  {
    return true;
  }

  if (selected_phase == "apply_updates" || selected_phase == "apply")
  {
    return hasPrefix(op_name, "pr_apply_");
  }

  if (selected_phase == "gen_updates" || selected_phase == "gen")
  {
    return hasPrefix(op_name, "pr_gen_");
  }

  if (selected_phase == "update_frontier" || selected_phase == "frontier")
  {
    return hasPrefix(op_name, "pr_frontier_");
  }

  return true;
}

[[nodiscard]] bool parseEnvFlag(const char* name, bool fallback)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
  {
    return fallback;
  }

  const std::string normalized(value);
  if (normalized == "0" || normalized == "false" || normalized == "False" || normalized == "FALSE")
  {
    return false;
  }

  return true;
}

[[nodiscard]] uint64_t parseEnvU64(const char* name, uint64_t fallback)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
  {
    return fallback;
  }

  try
  {
    return std::stoull(value);
  }
  catch (...)
  {
    return fallback;
  }
}

[[nodiscard]] uint16_t parseEnvU16(const char* name, uint16_t fallback)
{
  const uint64_t parsed = parseEnvU64(name, fallback);
  if (parsed > static_cast<uint64_t>(UINT16_MAX))
  {
    return fallback;
  }

  return static_cast<uint16_t>(parsed);
}

[[nodiscard]] std::string parseEnvString(const char* name, const std::string& fallback)
{
  const char* value = std::getenv(name);
  return (value == nullptr) ? fallback : std::string(value);
}

[[nodiscard]] std::string jsonEscape(const std::string& raw)
{
  std::ostringstream escaped;

  for (const char c : raw)
  {
    switch (c)
    {
      case '"':
        escaped << "\\\"";
        break;
      case '\\':
        escaped << "\\\\";
        break;
      case '\n':
        escaped << "\\n";
        break;
      case '\r':
        escaped << "\\r";
        break;
      case '\t':
        escaped << "\\t";
        break;
      default:
        escaped << c;
    }
  }

  return escaped.str();
}

}  // namespace

RuntimeProfiler::RuntimeProfiler(const std::string& algorithm_name, const uint32_t rank)
    : algorithm_name_(algorithm_name), rank_(rank)
{
  enabled_ = parseEnvFlag("GRUDON_ENABLE_PERF_PROFILE", true);
  pr_internal_enabled_ = parseEnvFlag("GRUDON_ENABLE_PR_INTERNAL_PROFILE", false);
  pr_fine_enabled_ = parseEnvFlag("GRUDON_ENABLE_PR_FINE_PROFILE", false);
  output_dir_ = parseEnvString("GRUDON_PROFILE_OUTPUT_DIR", "output");
  output_prefix_ = parseEnvString("GRUDON_PROFILE_PREFIX", "grudon");

  for (const char* op_name : baseOperationNames())
  {
    operation_names_.emplace_back(op_name);
  }

  if (algorithm_name_ == "PageRank" && pr_internal_enabled_)
  {
    const std::string selected_phase = normalizeAsciiLower(parseEnvString("GRUDON_PR_PROFILE_PHASE", "all"));

    if (pr_fine_enabled_)
    {
      for (const char* op_name : kPageRankFineOperationNames)
      {
        if (shouldEnablePhaseOperation(op_name, selected_phase))
        {
          operation_names_.emplace_back(op_name);
        }
      }
    }
    else
    {
      for (const char* op_name : kPageRankInternalOperationNames)
      {
        if (shouldEnablePhaseOperation(op_name, selected_phase))
        {
          operation_names_.emplace_back(op_name);
        }
      }
    }
  }

  for (const std::string& op_name : operation_names_)
  {
    call_counts_[op_name] = 0;
    operation_counters_[op_name] = {};
  }

#if defined(GRUDON_ENABLE_PERF_CPP)
  callgraph_enabled_ = parseEnvFlag("GRUDON_PERF_ENABLE_CALLGRAPH", true);
  callgraph_max_depth_ = parseEnvU16("GRUDON_PERF_CALLGRAPH_MAX_DEPTH", 0);
  initPerfCpp();
#endif
}

const std::array<const char*, 8>& RuntimeProfiler::baseOperationNames()
{
  return kOperationNames;
}

const std::vector<std::string>& RuntimeProfiler::operationNames() const
{
  return operation_names_;
}

bool RuntimeProfiler::hasOperation(const std::string& operation) const
{
  return std::find(operation_names_.begin(), operation_names_.end(), operation) != operation_names_.end();
}

void RuntimeProfiler::startIteration()
{
#if defined(GRUDON_ENABLE_PERF_CPP)
  if (!enabled_ || !sampler_ready_)
  {
    return;
  }

  try
  {
    sampler_.start();
    sampler_running_ = true;
  }
  catch (...)
  {
    sampler_running_ = false;
  }
#endif
}

void RuntimeProfiler::stopIteration()
{
#if defined(GRUDON_ENABLE_PERF_CPP)
  if (!enabled_ || !sampler_running_)
  {
    return;
  }

  try
  {
    sampler_.stop();
  }
  catch (...)
  {
  }

  sampler_running_ = false;
#endif
}

void RuntimeProfiler::writeTrace()
{
#if defined(GRUDON_ENABLE_PERF_CPP)
  if (!enabled_ || !sampler_ready_)
  {
    return;
  }

  try
  {
    const std::string trace_name = output_prefix_ + "_perf_trace_rank_" + std::to_string(rank_) + ".data";
    std::filesystem::create_directories(output_dir_);
    const auto trace_path = (std::filesystem::path(output_dir_) / trace_name).string();
    sampler_.to_perf_file(trace_path);
    updateCallGraphStats();
  }
  catch (...)
  {
  }
#endif
}

void RuntimeProfiler::increment(const std::string& operation)
{
  auto it = call_counts_.find(operation);
  if (it != call_counts_.end())
  {
    it->second++;
  }
}

bool RuntimeProfiler::startOperation(const std::string& operation)
{
#if defined(GRUDON_ENABLE_PERF_CPP)
  if (!enabled_)
  {
    return false;
  }

  auto it = operation_perf_counters_.find(operation);
  if (it == operation_perf_counters_.end())
  {
    return false;
  }

  try
  {
    return it->second.start();
  }
  catch (...)
  {
    return false;
  }
#else
  (void) operation;
  return false;
#endif
}

void RuntimeProfiler::stopOperation(const std::string& operation, const bool was_started)
{
  increment(operation);

#if defined(GRUDON_ENABLE_PERF_CPP)
  if (!enabled_ || !was_started)
  {
    return;
  }

  auto it = operation_perf_counters_.find(operation);
  if (it == operation_perf_counters_.end())
  {
    return;
  }

  try
  {
    it->second.stop();

    const auto result = it->second.result();
    for (const auto& [event_name, value] : result)
    {
      operation_counters_[operation][std::string(event_name)] += value;
    }
  }
  catch (...)
  {
  }
#else
  (void) was_started;
#endif
}

void RuntimeProfiler::addHostToRemoteBytes(const uint64_t bytes)
{
  host_to_remote_bytes_ += bytes;
}

void RuntimeProfiler::addRemoteToHostBytes(const uint64_t bytes)
{
  remote_to_host_bytes_ += bytes;
}

uint64_t RuntimeProfiler::hostToRemoteBytes() const
{
  return host_to_remote_bytes_;
}

uint64_t RuntimeProfiler::remoteToHostBytes() const
{
  return remote_to_host_bytes_;
}

uint64_t RuntimeProfiler::callCount(const std::string& op_name) const
{
  const auto it = call_counts_.find(op_name);
  return (it == call_counts_.end()) ? 0 : it->second;
}

uint64_t RuntimeProfiler::callGraphSamples() const
{
  return callgraph_samples_;
}

uint64_t RuntimeProfiler::callGraphFrames() const
{
  return callgraph_frames_;
}

bool RuntimeProfiler::isCallGraphEnabled() const
{
  return callgraph_enabled_;
}

uint16_t RuntimeProfiler::callGraphMaxDepth() const
{
  return callgraph_max_depth_;
}

void RuntimeProfiler::writeJson(
    const uint32_t world_size,
    const NODE_TYPE node_type,
    const uint64_t iterations,
    const uint64_t global_host_to_remote,
    const uint64_t global_remote_to_host,
    const std::unordered_map<std::string, uint64_t>& global_calls,
    const uint64_t global_callgraph_samples,
    const uint64_t global_callgraph_frames) const
{
  std::filesystem::create_directories(output_dir_);
  const auto json_path =
      std::filesystem::path(output_dir_) / (output_prefix_ + "_profile_rank_" + std::to_string(rank_) + ".json");

  std::ofstream json(json_path, std::ios::out | std::ios::trunc);
  if (!json.is_open())
  {
    return;
  }

  json << "{\n";
  json << "  \"algorithm\": \"" << jsonEscape(algorithm_name_) << "\",\n";
  json << "  \"rank\": " << rank_ << ",\n";
  json << "  \"world_size\": " << world_size << ",\n";
  json << "  \"node_type\": \"" << (node_type == COMPUTE_NODE ? "compute" : "memory") << "\",\n";
  json << "  \"iterations\": " << iterations << ",\n";

  json << "  \"events_enabled\": [";
  for (size_t i = 0; i < enabled_events_.size(); i++)
  {
    json << "\"" << jsonEscape(enabled_events_[i]) << "\"";
    if (i + 1 < enabled_events_.size())
    {
      json << ", ";
    }
  }
  json << "],\n";

  json << "  \"events_not_supported\": [";
  for (size_t i = 0; i < unsupported_events_.size(); i++)
  {
    json << "\"" << jsonEscape(unsupported_events_[i]) << "\"";
    if (i + 1 < unsupported_events_.size())
    {
      json << ", ";
    }
  }
  json << "],\n";

  json << "  \"call_graph\": {\n";
  json << "    \"enabled\": " << (callgraph_enabled_ ? "true" : "false") << ",\n";
  json << "    \"max_depth\": " << callgraph_max_depth_ << ",\n";
  json << "    \"local_sample_count\": " << callgraph_samples_ << ",\n";
  json << "    \"local_frame_count\": " << callgraph_frames_ << ",\n";
  json << "    \"global_sample_count\": " << global_callgraph_samples << ",\n";
  json << "    \"global_frame_count\": " << global_callgraph_frames << "\n";
  json << "  },\n";

  json << "  \"operation_call_counts\": {\n";
  const auto& op_names = operationNames();
  for (size_t i = 0; i < op_names.size(); i++)
  {
    const std::string& op = op_names[i];
    json << "    \"" << op << "\": " << callCount(op);
    if (i + 1 < op_names.size())
    {
      json << ",";
    }
    json << "\n";
  }
  json << "  },\n";

  json << "  \"global_operation_call_counts\": {\n";
  for (size_t i = 0; i < op_names.size(); i++)
  {
    const std::string& op = op_names[i];
    const auto it = global_calls.find(op);
    const uint64_t count = (it == global_calls.end()) ? 0 : it->second;
    json << "    \"" << op << "\": " << count;
    if (i + 1 < op_names.size())
    {
      json << ",";
    }
    json << "\n";
  }
  json << "  },\n";

  json << "  \"operation_counters\": {\n";
  for (size_t i = 0; i < op_names.size(); i++)
  {
    const std::string& op = op_names[i];
    json << "    \"" << op << "\": {";

    const auto op_it = operation_counters_.find(op);
    if (op_it != operation_counters_.end())
    {
      size_t emitted = 0;
      for (const auto& [event_name, value] : op_it->second)
      {
        json << "\"" << jsonEscape(event_name) << "\": " << value;
        emitted++;
        if (emitted < op_it->second.size())
        {
          json << ", ";
        }
      }
    }

    json << "}";
    if (i + 1 < op_names.size())
    {
      json << ",";
    }
    json << "\n";
  }
  json << "  },\n";

  json << "  \"data_movement_bytes\": {\n";
  json << "    \"host_to_remote\": " << host_to_remote_bytes_ << ",\n";
  json << "    \"remote_to_host\": " << remote_to_host_bytes_ << ",\n";
  json << "    \"total_local\": " << (host_to_remote_bytes_ + remote_to_host_bytes_) << "\n";
  json << "  },\n";

  json << "  \"global_data_movement_bytes\": {\n";
  json << "    \"host_to_remote\": " << global_host_to_remote << ",\n";
  json << "    \"remote_to_host\": " << global_remote_to_host << ",\n";
  json << "    \"total\": " << (global_host_to_remote + global_remote_to_host) << "\n";
  json << "  }\n";

  json << "}\n";
}

#if defined(GRUDON_ENABLE_PERF_CPP)
void RuntimeProfiler::tryAddEvent(perf::EventCounter& counter, const std::string& event_name)
{
  if (counter.add(event_name))
  {
    enabled_events_.push_back(event_name);
  }
  else
  {
    unsupported_events_.push_back(event_name);
  }
}

void RuntimeProfiler::initPerfCpp()
{
  if (!enabled_)
  {
    return;
  }

  std::string events_env = parseEnvString("GRUDON_PERF_EVENTS", "instructions,cycles,cache-misses");
  std::stringstream ss(events_env);
  std::string token;
  

  // Since `GEN_PROCESSOR_EVENTS` is set to ON in CMakeLists.txt, Intel Xeon-specific events
  // are already auto-generated and compiled into the global counter definitions!
  // If you still want to override or load a specific CSV dynamically without rebuilding, 
  // you can provide the absolute path using the GRUDON_PERF_EVENTS_CSV environment variable.
  std::string csv_path = parseEnvString("GRUDON_PERF_EVENTS_CSV", "");
  if (!csv_path.empty())
  {
    counter_definition_ = std::make_unique<perf::CounterDefinition>(csv_path);
  }

  perf::Config perf_config;
  perf_config.include_child_threads(true);

  perf::EventCounter base_counter = counter_definition_
                                        ? perf::EventCounter(*counter_definition_, perf_config)
                                        : perf::EventCounter(perf_config);

  while (std::getline(ss, token, ','))
  {
    if (!token.empty())
    {
      tryAddEvent(base_counter, token);
    }
  }

  std::string abbrev;
  for (const auto& ev : enabled_events_)
  {
    if (ev == "instructions") abbrev += "i";
    else if (ev == "cycles") abbrev += "c";
    else if (ev == "cache-misses") abbrev += "m";
    else if (ev == "branches") abbrev += "b";
    else if (ev == "branch-misses") abbrev += "bm";
    else if (ev == "page-faults") abbrev += "pf";
    else if (ev == "context-switches") abbrev += "cs";
    else if (ev == "cpu-migrations") abbrev += "cm";
    else if (ev == "L1-dcache-loads") abbrev += "L1l";
    else if (ev == "L1-dcache-load-misses") abbrev += "L1lm";
    else if (ev == "LLC-loads") abbrev += "LLCl";
    else if (ev == "LLC-load-misses") abbrev += "LLClm";
    else abbrev += ev[0];
  }

  if (!abbrev.empty())
  {
    output_prefix_ += "_" + abbrev;
  }

  for (const std::string& op_name : operation_names_)
  {
    auto it = operation_perf_counters_.emplace(op_name, perf::EventCounter::copy_from_template(base_counter)).first;
    it->second.open();
  }

  try
  {
    auto freq = perf::Frequency{parseEnvU64("GRUDON_PERF_SAMPLE_FREQUENCY", 10000ULL)};
    if (perf::HardwareInfo::is_intel())
    {
      sampler_.trigger("cycles", perf::Precision::MustHaveZeroSkid, freq);
    }
    else
    {
      sampler_.trigger(std::string("cycles"), freq);
    }

    auto& values = sampler_.values();
    values.thread_id(true).timestamp(true).extended_mmap_information(true).cpu_id(true).logical_instruction_pointer(true);

    if (callgraph_enabled_)
    {
      if (callgraph_max_depth_ > 0)
      {
        values.callchain(callgraph_max_depth_);
      }
      else
      {
        values.callchain(true);
      }
    }

    sampler_ready_ = true;
  }
  catch (...)
  {
    sampler_ready_ = false;
  }
  perf_config.is_pinned(true);
}

void RuntimeProfiler::updateCallGraphStats()
{
  if (!callgraph_enabled_)
  {
    return;
  }

  try
  {
    const auto samples = sampler_.result();

    callgraph_samples_ = 0;
    callgraph_frames_ = 0;

    for (const auto& sample : samples)
    {
      const auto& callchain = sample.instruction_execution().callchain();
      if (callchain.has_value())
      {
        callgraph_samples_++;
        callgraph_frames_ += callchain->size();
      }
    }
  }
  catch (...)
  {
  }
}
#endif

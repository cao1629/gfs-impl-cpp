#include "common/config.h"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <string_view>

namespace gfs {

uint64_t Config::effectiveMaxRecordAppendSize() const {
  return max_record_append_size == 0 ? chunk_size / 4 : max_record_append_size;
}

Millis Config::effectiveOuterRetryDelay() const {
  return outer_retry_delay.count() == 0 ? chunkserver_dead_timeout : outer_retry_delay;
}

std::string Config::effectiveAdvertise() const {
  return advertise.empty() ? listen : advertise;
}

bool parseDuration(const std::string& text, Millis* out) {
  size_t pos = 0;
  double value = 0;
  try {
    value = std::stod(text, &pos);
  } catch (const std::exception&) {
    return false;
  }
  std::string unit = text.substr(pos);
  double factor = 1;
  if (unit.empty() || unit == "ms") factor = 1;
  else if (unit == "s") factor = 1000;
  else if (unit == "m") factor = 60'000;
  else if (unit == "h") factor = 3'600'000;
  else if (unit == "d") factor = 86'400'000;
  else return false;
  *out = Millis(static_cast<int64_t>(value * factor));
  return true;
}

bool parseSize(const std::string& text, uint64_t* out) {
  size_t pos = 0;
  double value = 0;
  try {
    value = std::stod(text, &pos);
  } catch (const std::exception&) {
    return false;
  }
  std::string unit = text.substr(pos);
  double factor = 1;
  if (unit.empty()) factor = 1;
  else if (unit == "K" || unit == "KB" || unit == "k") factor = 1024.0;
  else if (unit == "M" || unit == "MB" || unit == "m") factor = 1024.0 * 1024;
  else if (unit == "G" || unit == "GB" || unit == "g") factor = 1024.0 * 1024 * 1024;
  else return false;
  *out = static_cast<uint64_t>(value * factor);
  return true;
}

namespace {

using Setter = std::function<bool(Config&, const std::string&)>;

Setter sizeSetter(uint64_t Config::*field) {
  return [field](Config& c, const std::string& v) { return parseSize(v, &(c.*field)); };
}

Setter countSetter(uint32_t Config::*field) {
  return [field](Config& c, const std::string& v) {
    uint64_t n = 0;
    if (!parseSize(v, &n) || n > UINT32_MAX) return false;
    c.*field = static_cast<uint32_t>(n);
    return true;
  };
}

Setter durationSetter(Millis Config::*field) {
  return [field](Config& c, const std::string& v) { return parseDuration(v, &(c.*field)); };
}

Setter stringSetter(std::string Config::*field) {
  return [field](Config& c, const std::string& v) { c.*field = v; return true; };
}

const std::map<std::string, Setter>& setters() {
  static const std::map<std::string, Setter> table = {
      {"chunk_size", sizeSetter(&Config::chunk_size)},
      {"max_record_append_size", sizeSetter(&Config::max_record_append_size)},
      {"replication_goal", countSetter(&Config::replication_goal)},
      {"min_replicas_for_write", countSetter(&Config::min_replicas_for_write)},
      {"checksum_block_size", sizeSetter(&Config::checksum_block_size)},
      {"lease_duration", durationSetter(&Config::lease_duration)},
      {"lease_clock_skew_margin", durationSetter(&Config::lease_clock_skew_margin)},
      {"heartbeat_interval", durationSetter(&Config::heartbeat_interval)},
      {"chunkserver_dead_timeout", durationSetter(&Config::chunkserver_dead_timeout)},
      {"deleted_file_retention", durationSetter(&Config::deleted_file_retention)},
      {"client_location_cache_ttl", durationSetter(&Config::client_location_cache_ttl)},
      {"gc_interval", durationSetter(&Config::gc_interval)},
      {"mutation_retry_inner", countSetter(&Config::mutation_retry_inner)},
      {"mutation_retry_outer", countSetter(&Config::mutation_retry_outer)},
      {"retry_backoff_base", durationSetter(&Config::retry_backoff_base)},
      {"outer_retry_delay", durationSetter(&Config::outer_retry_delay)},
      {"log_flush_batch_size", countSetter(&Config::log_flush_batch_size)},
      {"log_flush_max_delay", durationSetter(&Config::log_flush_max_delay)},
      {"checkpoint_log_threshold", sizeSetter(&Config::checkpoint_log_threshold)},
      {"rpc_deadline", durationSetter(&Config::rpc_deadline)},
      {"client_rpc_deadline", durationSetter(&Config::client_rpc_deadline)},
      {"master_worker_threads", countSetter(&Config::master_worker_threads)},
      {"push_frame_size", sizeSetter(&Config::push_frame_size)},
      {"data_buffer_capacity", sizeSetter(&Config::data_buffer_capacity)},
      {"rack", stringSetter(&Config::rack)},
      {"listen", stringSetter(&Config::listen)},
      {"advertise", stringSetter(&Config::advertise)},
      {"master_address", stringSetter(&Config::master_address)},
      {"data_dir", stringSetter(&Config::data_dir)},
  };
  return table;
}

}

bool Config::set(Config& config, const std::string& key, const std::string& value) {
  auto it = setters().find(key);
  if (it == setters().end()) return false;
  return it->second(config, value);
}

std::string Config::usage() {
  std::string text = "options (--key=value or --key value):\n";
  for (const auto& [key, _] : setters()) text += "  --" + key + "\n";
  return text;
}

Config Config::fromArgs(int argc, char** argv) {
  Config config;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg.substr(0, 2) != "--") {
      std::cerr << "unexpected argument: " << arg << "\n" << usage();
      std::exit(2);
    }
    arg.remove_prefix(2);
    std::string key, value;
    auto eq = arg.find('=');
    if (eq != std::string_view::npos) {
      key = std::string(arg.substr(0, eq));
      value = std::string(arg.substr(eq + 1));
    } else {
      key = std::string(arg);
      if (i + 1 >= argc) {
        std::cerr << "missing value for --" << key << "\n" << usage();
        std::exit(2);
      }
      value = argv[++i];
    }
    if (!set(config, key, value)) {
      std::cerr << "bad option --" << key << "=" << value << "\n" << usage();
      std::exit(2);
    }
  }
  return config;
}

}

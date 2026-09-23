#include "common/config.h"

#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <string_view>

namespace gfs {

uint64_t Config::EffectiveMaxRecordAppendSize() const {
  return max_record_append_size == 0 ? chunk_size / 4 : max_record_append_size;
}

Millis Config::EffectiveOuterRetryDelay() const {
  return outer_retry_delay.count() == 0 ? chunkserver_dead_timeout
                                        : outer_retry_delay;
}

std::string Config::EffectiveAdvertise() const {
  return advertise.empty() ? listen : advertise;
}

bool ParseDuration(const std::string& text, Millis* out) {
  size_t pos = 0;
  double value = 0;
  try {
    value = std::stod(text, &pos);
  } catch (const std::exception&) {
    return false;
  }
  std::string unit = text.substr(pos);
  double factor = 1;
  if (unit.empty() || unit == "ms")
    factor = 1;
  else if (unit == "s")
    factor = 1000;
  else if (unit == "m")
    factor = 60'000;
  else if (unit == "h")
    factor = 3'600'000;
  else if (unit == "d")
    factor = 86'400'000;
  else
    return false;
  *out = Millis(static_cast<int64_t>(value * factor));
  return true;
}

bool ParseSize(const std::string& text, uint64_t* out) {
  size_t pos = 0;
  double value = 0;
  try {
    value = std::stod(text, &pos);
  } catch (const std::exception&) {
    return false;
  }
  std::string unit = text.substr(pos);
  double factor = 1;
  if (unit.empty())
    factor = 1;
  else if (unit == "K" || unit == "KB" || unit == "k")
    factor = 1024.0;
  else if (unit == "M" || unit == "MB" || unit == "m")
    factor = 1024.0 * 1024;
  else if (unit == "G" || unit == "GB" || unit == "g")
    factor = 1024.0 * 1024 * 1024;
  else
    return false;
  *out = static_cast<uint64_t>(value * factor);
  return true;
}

namespace {

using Setter = std::function<bool(Config&, const std::string&)>;

Setter SizeSetter(uint64_t Config::* field) {
  return [field](Config& c, const std::string& v) {
    return ParseSize(v, &(c.*field));
  };
}

Setter CountSetter(uint32_t Config::* field) {
  return [field](Config& c, const std::string& v) {
    uint64_t n = 0;
    if (!ParseSize(v, &n) || n > UINT32_MAX) return false;
    c.*field = static_cast<uint32_t>(n);
    return true;
  };
}

Setter DurationSetter(Millis Config::* field) {
  return [field](Config& c, const std::string& v) {
    return ParseDuration(v, &(c.*field));
  };
}

Setter StringSetter(std::string Config::* field) {
  return [field](Config& c, const std::string& v) {
    c.*field = v;
    return true;
  };
}

const std::map<std::string, Setter>& Setters() {
  static const std::map<std::string, Setter> table = {
      {"chunk_size", SizeSetter(&Config::chunk_size)},
      {"max_record_append_size", SizeSetter(&Config::max_record_append_size)},
      {"replication_goal", CountSetter(&Config::replication_goal)},
      {"min_replicas_for_write", CountSetter(&Config::min_replicas_for_write)},
      {"checksum_block_size", SizeSetter(&Config::checksum_block_size)},
      {"lease_duration", DurationSetter(&Config::lease_duration)},
      {"lease_clock_skew_margin",
       DurationSetter(&Config::lease_clock_skew_margin)},
      {"heartbeat_interval", DurationSetter(&Config::heartbeat_interval)},
      {"chunkserver_dead_timeout",
       DurationSetter(&Config::chunkserver_dead_timeout)},
      {"deleted_file_retention",
       DurationSetter(&Config::deleted_file_retention)},
      {"client_location_cache_ttl",
       DurationSetter(&Config::client_location_cache_ttl)},
      {"gc_interval", DurationSetter(&Config::gc_interval)},
      {"mutation_retry_inner", CountSetter(&Config::mutation_retry_inner)},
      {"mutation_retry_outer", CountSetter(&Config::mutation_retry_outer)},
      {"retry_backoff_base", DurationSetter(&Config::retry_backoff_base)},
      {"outer_retry_delay", DurationSetter(&Config::outer_retry_delay)},
      {"log_flush_batch_size", CountSetter(&Config::log_flush_batch_size)},
      {"log_flush_max_delay", DurationSetter(&Config::log_flush_max_delay)},
      {"checkpoint_log_threshold",
       SizeSetter(&Config::checkpoint_log_threshold)},
      {"rpc_deadline", DurationSetter(&Config::rpc_deadline)},
      {"client_rpc_deadline", DurationSetter(&Config::client_rpc_deadline)},
      {"master_worker_threads", CountSetter(&Config::master_worker_threads)},
      {"push_frame_size", SizeSetter(&Config::push_frame_size)},
      {"data_buffer_capacity", SizeSetter(&Config::data_buffer_capacity)},
      {"rack", StringSetter(&Config::rack)},
      {"listen", StringSetter(&Config::listen)},
      {"advertise", StringSetter(&Config::advertise)},
      {"master_address", StringSetter(&Config::master_address)},
      {"data_dir", StringSetter(&Config::data_dir)},
  };
  return table;
}

}  // namespace

bool Config::Set(Config& config, const std::string& key,
                 const std::string& value) {
  auto it = Setters().find(key);
  if (it == Setters().end()) return false;
  return it->second(config, value);
}

std::string Config::Usage() {
  std::string text = "options (--key=value or --key value):\n";
  for (const auto& [key, _] : Setters()) text += "  --" + key + "\n";
  return text;
}

Config Config::FromArgs(int argc, char** argv) {
  Config config;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg(argv[i]);
    if (arg.substr(0, 2) != "--") {
      std::cerr << "unexpected argument: " << arg << "\n" << Usage();
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
        std::cerr << "missing value for --" << key << "\n" << Usage();
        std::exit(2);
      }
      value = argv[++i];
    }
    if (!Set(config, key, value)) {
      std::cerr << "bad option --" << key << "=" << value << "\n" << Usage();
      std::exit(2);
    }
  }
  return config;
}

}  // namespace gfs

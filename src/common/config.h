#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace gfs {

using Millis = std::chrono::milliseconds;

struct Config {
  uint64_t chunk_size = 64ull << 20;
  uint64_t max_record_append_size = 0;
  uint32_t replication_goal = 3;
  uint32_t min_replicas_for_write = 1;
  uint64_t checksum_block_size = 64ull << 10;

  Millis lease_duration{60'000};
  Millis lease_clock_skew_margin{5'000};
  Millis heartbeat_interval{3'000};
  Millis chunkserver_dead_timeout{15'000};
  Millis deleted_file_retention{3LL * 24 * 3600 * 1000};
  Millis client_location_cache_ttl{60'000};
  Millis gc_interval{60'000};

  uint32_t mutation_retry_inner = 3;
  uint32_t mutation_retry_outer = 2;
  Millis retry_backoff_base{200};
  Millis outer_retry_delay{0};

  uint32_t log_flush_batch_size = 32;
  Millis log_flush_max_delay{10};
  uint64_t checkpoint_log_threshold = 64ull << 20;

  Millis rpc_deadline{2'000};
  Millis client_rpc_deadline{10'000};
  uint32_t master_worker_threads = 16;
  uint64_t push_frame_size = 256ull << 10;
  uint64_t data_buffer_capacity = 64ull << 20;
  std::string rack;

  std::string listen = "127.0.0.1:7000";
  std::string advertise;
  std::string master_address = "127.0.0.1:7000";
  std::string data_dir;

  uint64_t effectiveMaxRecordAppendSize() const;
  Millis effectiveOuterRetryDelay() const;
  std::string effectiveAdvertise() const;

  static Config fromArgs(int argc, char** argv);
  static bool set(Config& config, const std::string& key, const std::string& value);
  static std::string usage();
};

bool parseDuration(const std::string& text, Millis* out);
bool parseSize(const std::string& text, uint64_t* out);

}

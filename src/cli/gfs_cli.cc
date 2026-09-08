#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "client/gfs_client.h"

namespace {

const char* kUsage =
    "usage: gfs [--key=value ...] <command> [args...]\n"
    "commands:\n"
    "  create <path>\n"
    "  rm <path>\n"
    "  mv <source> <target>\n"
    "  snapshot <source> <target>\n"
    "  ls [-a] <directory>\n"
    "  stat <path>\n"
    "  read <path> [offset] [length]\n"
    "  write <path> [offset]\n"
    "  append <path>\n";

const char* codeName(gfs::ErrorCode code) {
  switch (code) {
    case gfs::ErrorCode::kOk: return "ok";
    case gfs::ErrorCode::kNotFound: return "not found";
    case gfs::ErrorCode::kAlreadyExists: return "already exists";
    case gfs::ErrorCode::kInvalidArgument: return "invalid argument";
    case gfs::ErrorCode::kUnavailable: return "unavailable";
    case gfs::ErrorCode::kStale: return "stale";
    case gfs::ErrorCode::kFailed: return "failed";
  }
  return "failed";
}

int fail(const std::string& message) {
  std::cerr << message << std::endl;
  return 1;
}

int usage() {
  std::cerr << kUsage << gfs::Config::usage();
  return 2;
}

bool parseNumber(const std::string& text, uint64_t* out) {
  return gfs::parseSize(text, out);
}

std::string readStdin() {
  return std::string(std::istreambuf_iterator<char>(std::cin), std::istreambuf_iterator<char>());
}

int check(const gfs::Status& status) {
  if (status.ok()) return 0;
  return fail(std::string(codeName(status.code)) + (status.message.empty() ? "" : ": " + status.message));
}

}

int main(int argc, char** argv) {
  gfs::Config config;
  std::vector<std::string> positional;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--", 0) != 0) {
      positional.push_back(arg);
      continue;
    }
    std::string key = arg.substr(2);
    std::string value;
    auto eq = key.find('=');
    if (eq != std::string::npos) {
      value = key.substr(eq + 1);
      key = key.substr(0, eq);
    } else if (i + 1 < argc) {
      value = argv[++i];
    } else {
      return usage();
    }
    if (!gfs::Config::set(config, key, value)) {
      std::cerr << "unknown option --" << key << "\n";
      return usage();
    }
  }
  if (positional.empty()) return usage();
  std::string command = positional[0];
  std::vector<std::string> args(positional.begin() + 1, positional.end());
  gfs::Client client(config);

  if (command == "create" && args.size() == 1) return check(client.create(args[0]));
  if (command == "rm" && args.size() == 1) return check(client.remove(args[0]));
  if (command == "mv" && args.size() == 2) return check(client.rename(args[0], args[1]));
  if (command == "snapshot" && args.size() == 2) return check(client.snapshot(args[0], args[1]));
  if (command == "ls" && (args.size() == 1 || (args.size() == 2 && args[0] == "-a"))) {
    bool all = args.size() == 2;
    std::vector<gfs::DirEntry> entries;
    gfs::Status status = client.list(args.back(), &entries, all);
    if (!status.ok()) return check(status);
    for (const auto& e : entries) std::cout << e.name << (e.is_directory ? "/" : "") << "\n";
    return 0;
  }
  if (command == "stat" && args.size() == 1) {
    gfs::FileInfo info;
    gfs::Status status = client.open(args[0], &info);
    if (!status.ok()) return check(status);
    uint64_t length = 0;
    status = client.length(args[0], &length);
    if (!status.ok()) return check(status);
    std::cout << "chunks: " << info.chunk_count << "\nlength: " << length << "\n";
    return 0;
  }
  if (command == "read" && args.size() >= 1 && args.size() <= 3) {
    uint64_t offset = 0;
    uint64_t length = 0;
    bool bounded = args.size() == 3;
    if (args.size() >= 2 && !parseNumber(args[1], &offset)) return fail("bad offset");
    if (bounded && !parseNumber(args[2], &length)) return fail("bad length");
    const uint64_t step = 1 << 20;
    uint64_t remaining = bounded ? length : step;
    while (remaining > 0) {
      uint64_t want = bounded ? std::min(remaining, step) : step;
      std::string data;
      gfs::Status status = client.read(args[0], offset, want, &data);
      if (!status.ok()) return check(status);
      std::cout.write(data.data(), static_cast<std::streamsize>(data.size()));
      offset += data.size();
      if (bounded) remaining -= std::min(remaining, want);
      if (data.size() < want) break;
    }
    std::cout.flush();
    return 0;
  }
  if (command == "write" && (args.size() == 1 || args.size() == 2)) {
    uint64_t offset = 0;
    if (args.size() == 2 && !parseNumber(args[1], &offset)) return fail("bad offset");
    return check(client.write(args[0], offset, readStdin()));
  }
  if (command == "append" && args.size() == 1) {
    uint64_t offset = 0;
    gfs::Status status = client.recordAppend(args[0], readStdin(), &offset);
    if (!status.ok()) return check(status);
    std::cout << offset << "\n";
    return 0;
  }
  return usage();
}

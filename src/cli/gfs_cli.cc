#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "client/gfs_client.h"

namespace {

const char* k_usage =
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

const char* CodeName(gfs::ErrorCode code) {
  switch (code) {
    case gfs::ErrorCode::kOk:
      return "ok";
    case gfs::ErrorCode::kNotFound:
      return "not found";
    case gfs::ErrorCode::kAlreadyExists:
      return "already exists";
    case gfs::ErrorCode::kInvalidArgument:
      return "invalid argument";
    case gfs::ErrorCode::kUnavailable:
      return "unavailable";
    case gfs::ErrorCode::kStale:
      return "stale";
    case gfs::ErrorCode::kFailed:
      return "failed";
  }
  return "failed";
}

int Fail(const std::string& message) {
  std::cerr << message << std::endl;
  return 1;
}

int Usage() {
  std::cerr << k_usage << gfs::Config::Usage();
  return 2;
}

bool ParseNumber(const std::string& text, uint64_t* out) {
  return gfs::ParseSize(text, out);
}

std::string ReadStdin() {
  return std::string(std::istreambuf_iterator<char>(std::cin),
                     std::istreambuf_iterator<char>());
}

int Check(const gfs::Status& status) {
  if (status.ok()) return 0;
  return Fail(std::string(CodeName(status.code)) +
              (status.message.empty() ? "" : ": " + status.message));
}

}  // namespace

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
      return Usage();
    }
    if (!gfs::Config::Set(config, key, value)) {
      std::cerr << "unknown option --" << key << "\n";
      return Usage();
    }
  }
  if (positional.empty()) return Usage();
  std::string command = positional[0];
  std::vector<std::string> args(positional.begin() + 1, positional.end());
  gfs::Client client(config);

  if (command == "create" && args.size() == 1)
    return Check(client.Create(args[0]));
  if (command == "rm" && args.size() == 1) return Check(client.Remove(args[0]));
  if (command == "mv" && args.size() == 2)
    return Check(client.Rename(args[0], args[1]));
  if (command == "snapshot" && args.size() == 2)
    return Check(client.Snapshot(args[0], args[1]));
  if (command == "ls" &&
      (args.size() == 1 || (args.size() == 2 && args[0] == "-a"))) {
    bool all = args.size() == 2;
    std::vector<gfs::DirEntry> entries;
    gfs::Status status = client.List(args.back(), &entries, all);
    if (!status.ok()) return Check(status);
    for (const auto& e : entries)
      std::cout << e.name << (e.is_directory ? "/" : "") << "\n";
    return 0;
  }
  if (command == "stat" && args.size() == 1) {
    gfs::FileInfo info;
    gfs::Status status = client.Open(args[0], &info);
    if (!status.ok()) return Check(status);
    uint64_t length = 0;
    status = client.Length(args[0], &length);
    if (!status.ok()) return Check(status);
    std::cout << "chunks: " << info.chunk_count << "\nlength: " << length
              << "\n";
    return 0;
  }
  if (command == "read" && args.size() >= 1 && args.size() <= 3) {
    uint64_t offset = 0;
    uint64_t length = 0;
    bool bounded = args.size() == 3;
    if (args.size() >= 2 && !ParseNumber(args[1], &offset))
      return Fail("bad offset");
    if (bounded && !ParseNumber(args[2], &length)) return Fail("bad length");
    const uint64_t step = 1 << 20;
    uint64_t remaining = bounded ? length : step;
    while (remaining > 0) {
      uint64_t want = bounded ? std::min(remaining, step) : step;
      std::string data;
      gfs::Status status = client.Read(args[0], offset, want, &data);
      if (!status.ok()) return Check(status);
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
    if (args.size() == 2 && !ParseNumber(args[1], &offset))
      return Fail("bad offset");
    return Check(client.Write(args[0], offset, ReadStdin()));
  }
  if (command == "append" && args.size() == 1) {
    uint64_t offset = 0;
    gfs::Status status = client.RecordAppend(args[0], ReadStdin(), &offset);
    if (!status.ok()) return Check(status);
    std::cout << offset << "\n";
    return 0;
  }
  return Usage();
}

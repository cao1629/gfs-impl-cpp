#include "client/gfs_client.h"

namespace gfs {

class Client::Impl {
 public:
  explicit Impl(Config config) : config_(std::move(config)) {}
  Config config_;
  std::string client_id_;
};

Client::Client(Config config) : impl_(std::make_unique<Impl>(std::move(config))) {}
Client::~Client() = default;

Status Client::create(const std::string&) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::remove(const std::string&) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::rename(const std::string&, const std::string&) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::snapshot(const std::string&, const std::string&) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::list(const std::string&, std::vector<DirEntry>*, bool) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::open(const std::string&, FileInfo*) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::length(const std::string&, uint64_t*) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::read(const std::string&, uint64_t, uint64_t, std::string*) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::write(const std::string&, uint64_t, std::string_view) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
Status Client::recordAppend(const std::string&, std::string_view, uint64_t*) { return Status::Error(ErrorCode::kFailed, "not implemented"); }
uint64_t Client::chunkSize() { return impl_->config_.chunk_size; }
const std::string& Client::clientId() const { return impl_->client_id_; }

}

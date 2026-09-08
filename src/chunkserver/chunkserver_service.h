#pragma once

#include <grpcpp/grpcpp.h>

#include "chunkserver/chunkserver.h"
#include "gfs.grpc.pb.h"

namespace gfs {

class ChunkserverService final : public rpc::Chunkserver::Service {
 public:
  explicit ChunkserverService(Chunkserver* chunkserver) : chunkserver_(chunkserver) {}

  grpc::Status PushData(grpc::ServerContext* ctx, grpc::ServerReader<rpc::PushDataRequest>* reader, rpc::PushDataResponse* resp) override;
  grpc::Status Read(grpc::ServerContext* ctx, const rpc::ReadRequest* req, rpc::ReadResponse* resp) override;
  grpc::Status Write(grpc::ServerContext* ctx, const rpc::WriteRequest* req, rpc::WriteResponse* resp) override;
  grpc::Status RecordAppend(grpc::ServerContext* ctx, const rpc::RecordAppendRequest* req, rpc::RecordAppendResponse* resp) override;
  grpc::Status GetChunkLength(grpc::ServerContext* ctx, const rpc::GetChunkLengthRequest* req, rpc::GetChunkLengthResponse* resp) override;
  grpc::Status ApplyMutation(grpc::ServerContext* ctx, const rpc::ApplyMutationRequest* req, rpc::ApplyMutationResponse* resp) override;
  grpc::Status CreateChunk(grpc::ServerContext* ctx, const rpc::CreateChunkRequest* req, rpc::CreateChunkResponse* resp) override;
  grpc::Status GrantLease(grpc::ServerContext* ctx, const rpc::GrantLeaseRequest* req, rpc::GrantLeaseResponse* resp) override;
  grpc::Status RevokeLease(grpc::ServerContext* ctx, const rpc::RevokeLeaseRequest* req, rpc::RevokeLeaseResponse* resp) override;
  grpc::Status UpdateVersion(grpc::ServerContext* ctx, const rpc::UpdateVersionRequest* req, rpc::UpdateVersionResponse* resp) override;

 private:
  Chunkserver* chunkserver_;
};

std::unique_ptr<grpc::Server> startChunkserverServer(ChunkserverService* service, const std::string& listen, uint64_t chunk_size, int* selected_port);

}

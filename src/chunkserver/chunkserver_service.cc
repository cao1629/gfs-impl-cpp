#include "chunkserver/chunkserver_service.h"

#include <algorithm>
#include <climits>

namespace gfs {

grpc::Status ChunkserverService::PushData(
    grpc::ServerContext*, grpc::ServerReader<rpc::PushDataRequest>* reader,
    rpc::PushDataResponse* resp) {
  *resp = chunkserver_->PushData(reader);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::Read(grpc::ServerContext*,
                                      const rpc::ReadRequest* req,
                                      rpc::ReadResponse* resp) {
  *resp = chunkserver_->Read(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::Write(grpc::ServerContext*,
                                       const rpc::WriteRequest* req,
                                       rpc::WriteResponse* resp) {
  *resp = chunkserver_->Write(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::RecordAppend(
    grpc::ServerContext*, const rpc::RecordAppendRequest* req,
    rpc::RecordAppendResponse* resp) {
  *resp = chunkserver_->RecordAppend(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::GetChunkLength(
    grpc::ServerContext*, const rpc::GetChunkLengthRequest* req,
    rpc::GetChunkLengthResponse* resp) {
  *resp = chunkserver_->GetChunkLength(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::ApplyMutation(
    grpc::ServerContext*, const rpc::ApplyMutationRequest* req,
    rpc::ApplyMutationResponse* resp) {
  *resp = chunkserver_->ApplyMutation(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::CreateChunk(grpc::ServerContext*,
                                             const rpc::CreateChunkRequest* req,
                                             rpc::CreateChunkResponse* resp) {
  *resp = chunkserver_->CreateChunk(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::GrantLease(grpc::ServerContext*,
                                            const rpc::GrantLeaseRequest* req,
                                            rpc::GrantLeaseResponse* resp) {
  *resp = chunkserver_->GrantLease(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::RevokeLease(grpc::ServerContext*,
                                             const rpc::RevokeLeaseRequest* req,
                                             rpc::RevokeLeaseResponse* resp) {
  *resp = chunkserver_->RevokeLease(*req);
  return grpc::Status::OK;
}

grpc::Status ChunkserverService::UpdateVersion(
    grpc::ServerContext*, const rpc::UpdateVersionRequest* req,
    rpc::UpdateVersionResponse* resp) {
  *resp = chunkserver_->UpdateVersion(*req);
  return grpc::Status::OK;
}

std::unique_ptr<grpc::Server> StartChunkserverServer(
    ChunkserverService* service, const std::string& listen, uint64_t chunk_size,
    int* selected_port) {
  int limit =
      static_cast<int>(std::min<uint64_t>(chunk_size + (1 << 20), INT_MAX));
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials(),
                           selected_port);
  builder.RegisterService(service);
  builder.SetMaxReceiveMessageSize(limit);
  builder.SetMaxSendMessageSize(limit);
  return builder.BuildAndStart();
}

}  // namespace gfs

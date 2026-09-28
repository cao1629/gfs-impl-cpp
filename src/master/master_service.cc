#include "master/master_service.h"

namespace gfs {

grpc::ServerUnaryReactor* MasterService::GetClusterInfo(
    grpc::CallbackServerContext* ctx, const rpc::GetClusterInfoRequest* req,
    rpc::GetClusterInfoResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::GetClusterInfo);
}

grpc::ServerUnaryReactor* MasterService::Create(
    grpc::CallbackServerContext* ctx, const rpc::CreateRequest* req,
    rpc::CreateResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::Create);
}

grpc::ServerUnaryReactor* MasterService::Open(grpc::CallbackServerContext* ctx,
                                              const rpc::OpenRequest* req,
                                              rpc::OpenResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::Open);
}

grpc::ServerUnaryReactor* MasterService::Delete(
    grpc::CallbackServerContext* ctx, const rpc::DeleteRequest* req,
    rpc::DeleteResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::Remove);
}

grpc::ServerUnaryReactor* MasterService::Rename(
    grpc::CallbackServerContext* ctx, const rpc::RenameRequest* req,
    rpc::RenameResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::Rename);
}

grpc::ServerUnaryReactor* MasterService::Snapshot(
    grpc::CallbackServerContext* ctx, const rpc::SnapshotRequest* req,
    rpc::SnapshotResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::Snapshot);
}

grpc::ServerUnaryReactor* MasterService::FindMatchingFiles(
    grpc::CallbackServerContext* ctx, const rpc::FindMatchingFilesRequest* req,
    rpc::FindMatchingFilesResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::FindMatchingFiles);
}

grpc::ServerUnaryReactor* MasterService::FindLocation(
    grpc::CallbackServerContext* ctx, const rpc::FindLocationRequest* req,
    rpc::FindLocationResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::FindLocation);
}

grpc::ServerUnaryReactor* MasterService::FindLeaseHolder(
    grpc::CallbackServerContext* ctx, const rpc::FindLeaseHolderRequest* req,
    rpc::FindLeaseHolderResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::FindLeaseHolder);
}

grpc::ServerUnaryReactor* MasterService::AddChunk(
    grpc::CallbackServerContext* ctx, const rpc::AddChunkRequest* req,
    rpc::AddChunkResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::AddChunk);
}

grpc::ServerUnaryReactor* MasterService::HeartBeat(
    grpc::CallbackServerContext* ctx, const rpc::HeartBeatRequest* req,
    rpc::HeartBeatResponse* resp) {
  return Dispatch(ctx, req, resp, &Master::HeartBeat);
}

}  // namespace gfs

#include "master/master_service.h"

namespace gfs {

grpc::ServerUnaryReactor* MasterService::GetClusterInfo(grpc::CallbackServerContext* ctx, const rpc::GetClusterInfoRequest* req, rpc::GetClusterInfoResponse* resp) {
  return dispatch(ctx, req, resp, &Master::getClusterInfo);
}

grpc::ServerUnaryReactor* MasterService::Create(grpc::CallbackServerContext* ctx, const rpc::CreateRequest* req, rpc::CreateResponse* resp) {
  return dispatch(ctx, req, resp, &Master::create);
}

grpc::ServerUnaryReactor* MasterService::Open(grpc::CallbackServerContext* ctx, const rpc::OpenRequest* req, rpc::OpenResponse* resp) {
  return dispatch(ctx, req, resp, &Master::open);
}

grpc::ServerUnaryReactor* MasterService::Delete(grpc::CallbackServerContext* ctx, const rpc::DeleteRequest* req, rpc::DeleteResponse* resp) {
  return dispatch(ctx, req, resp, &Master::remove);
}

grpc::ServerUnaryReactor* MasterService::Rename(grpc::CallbackServerContext* ctx, const rpc::RenameRequest* req, rpc::RenameResponse* resp) {
  return dispatch(ctx, req, resp, &Master::rename);
}

grpc::ServerUnaryReactor* MasterService::Snapshot(grpc::CallbackServerContext* ctx, const rpc::SnapshotRequest* req, rpc::SnapshotResponse* resp) {
  return dispatch(ctx, req, resp, &Master::snapshot);
}

grpc::ServerUnaryReactor* MasterService::FindMatchingFiles(grpc::CallbackServerContext* ctx, const rpc::FindMatchingFilesRequest* req, rpc::FindMatchingFilesResponse* resp) {
  return dispatch(ctx, req, resp, &Master::findMatchingFiles);
}

grpc::ServerUnaryReactor* MasterService::FindLocation(grpc::CallbackServerContext* ctx, const rpc::FindLocationRequest* req, rpc::FindLocationResponse* resp) {
  return dispatch(ctx, req, resp, &Master::findLocation);
}

grpc::ServerUnaryReactor* MasterService::FindLeaseHolder(grpc::CallbackServerContext* ctx, const rpc::FindLeaseHolderRequest* req, rpc::FindLeaseHolderResponse* resp) {
  return dispatch(ctx, req, resp, &Master::findLeaseHolder);
}

grpc::ServerUnaryReactor* MasterService::AddChunk(grpc::CallbackServerContext* ctx, const rpc::AddChunkRequest* req, rpc::AddChunkResponse* resp) {
  return dispatch(ctx, req, resp, &Master::addChunk);
}

grpc::ServerUnaryReactor* MasterService::HeartBeat(grpc::CallbackServerContext* ctx, const rpc::HeartBeatRequest* req, rpc::HeartBeatResponse* resp) {
  return dispatch(ctx, req, resp, &Master::heartBeat);
}

}

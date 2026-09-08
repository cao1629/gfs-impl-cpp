#pragma once

#include <grpcpp/grpcpp.h>

#include "gfs.grpc.pb.h"
#include "master/master.h"

namespace gfs {

class MasterService final : public rpc::Master::CallbackService {
 public:
  explicit MasterService(Master& master) : master_(master) {}

  grpc::ServerUnaryReactor* GetClusterInfo(grpc::CallbackServerContext* ctx, const rpc::GetClusterInfoRequest* req, rpc::GetClusterInfoResponse* resp) override;
  grpc::ServerUnaryReactor* Create(grpc::CallbackServerContext* ctx, const rpc::CreateRequest* req, rpc::CreateResponse* resp) override;
  grpc::ServerUnaryReactor* Open(grpc::CallbackServerContext* ctx, const rpc::OpenRequest* req, rpc::OpenResponse* resp) override;
  grpc::ServerUnaryReactor* Delete(grpc::CallbackServerContext* ctx, const rpc::DeleteRequest* req, rpc::DeleteResponse* resp) override;
  grpc::ServerUnaryReactor* Rename(grpc::CallbackServerContext* ctx, const rpc::RenameRequest* req, rpc::RenameResponse* resp) override;
  grpc::ServerUnaryReactor* Snapshot(grpc::CallbackServerContext* ctx, const rpc::SnapshotRequest* req, rpc::SnapshotResponse* resp) override;
  grpc::ServerUnaryReactor* FindMatchingFiles(grpc::CallbackServerContext* ctx, const rpc::FindMatchingFilesRequest* req, rpc::FindMatchingFilesResponse* resp) override;
  grpc::ServerUnaryReactor* FindLocation(grpc::CallbackServerContext* ctx, const rpc::FindLocationRequest* req, rpc::FindLocationResponse* resp) override;
  grpc::ServerUnaryReactor* FindLeaseHolder(grpc::CallbackServerContext* ctx, const rpc::FindLeaseHolderRequest* req, rpc::FindLeaseHolderResponse* resp) override;
  grpc::ServerUnaryReactor* AddChunk(grpc::CallbackServerContext* ctx, const rpc::AddChunkRequest* req, rpc::AddChunkResponse* resp) override;
  grpc::ServerUnaryReactor* HeartBeat(grpc::CallbackServerContext* ctx, const rpc::HeartBeatRequest* req, rpc::HeartBeatResponse* resp) override;

 private:
  template <typename Req, typename Resp, typename Method>
  grpc::ServerUnaryReactor* dispatch(grpc::CallbackServerContext* ctx, const Req* req, Resp* resp, Method method) {
    grpc::ServerUnaryReactor* reactor = ctx->DefaultReactor();
    master_.post([this, req, resp, reactor, method] {
      (master_.*method)(*req, resp, [reactor](grpc::Status status) { reactor->Finish(status); });
    });
    return reactor;
  }

  Master& master_;
};

}

#pragma once

#include <memory>
#include <string>

#include <spark/connect/base.grpc.pb.h>

#include "dataframe.h"
#include "ml/param/param_map.h"

using namespace spark::connect;

class Word2VecModel
{
  public:
    Word2VecModel(std::shared_ptr<SparkConnectService::Stub> stub, std::string session_id,
                  std::string user_id, ObjectRef obj_ref, ParamMap params)
        : stub_(std::move(stub))
        , session_id_(std::move(session_id))
        , user_id_(std::move(user_id))
        , obj_ref_(std::move(obj_ref))
        , params_(std::move(params))
    {
    }

    ~Word2VecModel() = default;

    DataFrame getVectors() const;
    DataFrame transform(const DataFrame& input_df) const;

    const ObjectRef& obj_ref() const
    {
        return obj_ref_;
    }

    const std::string& session_id() const
    {
        return session_id_;
    }

    const std::string& user_id() const
    {
        return user_id_;
    }

    std::shared_ptr<SparkConnectService::Stub> stub() const
    {
        return stub_;
    }

  private:
    std::shared_ptr<SparkConnectService::Stub> stub_;
    std::string session_id_;
    std::string user_id_;
    ObjectRef obj_ref_;
    ParamMap params_;
};

#pragma once

#include "dataframe.h"
#include "ml/param/param.h"
#include "ml/param/param_map.h"

#include <memory>
#include <spark/connect/base.grpc.pb.h>
#include <string>

using namespace spark::connect;

class Word2VecModel
{
  public:
    Word2VecModel(std::shared_ptr<SparkConnectService::Stub> stub,
                  std::string session_id, std::string user_id,
                  ObjectRef obj_ref, ParamMap params)
        : stub_(std::move(stub))
        , session_id_(std::move(session_id))
        , user_id_(std::move(user_id))
        , obj_ref_(std::move(obj_ref))
        , params_(std::move(params))
        , input_column_(uid_, "inputCol",
                        "The name of the input column. Default is \"\".",
                        param_is_valid_string)
        , output_column_(uid_, "outputCol",
                         "The name of the output column. Default is \"\".",
                         param_is_valid_string)
    {
    }

    ObjectRef const&
    obj_ref() const
    {
        return obj_ref_;
    }

    std::string const&
    session_id() const
    {
        return session_id_;
    }

    std::string const&
    user_id() const
    {
        return user_id_;
    }

    std::shared_ptr<SparkConnectService::Stub>
    stub() const
    {
        return stub_;
    }

    std::string
    input_col() const
    {
        return params_.getOrElse(input_column_, std::string{});
    }

    std::string
    output_col() const
    {
        return params_.getOrElse(output_column_, std::string{});
    }

    void
    set_input_col(std::string const& input_col)
    {
        params_.put(input_column_, input_col);
    }

    void
    set_output_col(std::string const& output_col)
    {
        params_.put(output_column_, output_col);
    }

    DataFrame getVectors() const;

    DataFrame transform(DataFrame const& input_df) const;

  private:
    std::string const uid_ =
        "Word2VecModel_" +
        std::to_string(reinterpret_cast<std::uintptr_t>(this));

    std::shared_ptr<SparkConnectService::Stub> stub_;
    std::string session_id_;
    std::string user_id_;
    ObjectRef obj_ref_;
    ParamMap params_;

    Param<std::string> input_column_;
    Param<std::string> output_column_;
};

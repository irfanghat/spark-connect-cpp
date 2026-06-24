#include <stdexcept>

#include <grpcpp/grpcpp.h>

#include "word_2_vec.h"
#include "word_2_vec_model.h"
#include "ml/param/param_map.h"

Word2VecModel Word2Vec::fit(const DataFrame& input_df)
{
    if (!input_df.plan().has_root())
    {
        throw std::invalid_argument("Input DataFrame has no root relation");
    }

    ExecutePlanRequest request;
    request.set_session_id(input_df.session_id());
    request.mutable_user_context()->set_user_id(input_df.user_id());

    // ----------------------------------------------------------------------
    // Build Plan -> Command -> MlCommand -> Fit
    // ----------------------------------------------------------------------
    auto* mutable_fit = request.mutable_plan()
                    ->mutable_command()
                    ->mutable_ml_command()
                    ->mutable_fit();

    // ----------------------------------------------------------------------
    // Set Ml Operator (estimator)
    // ----------------------------------------------------------------------
    auto* estimator = mutable_fit->mutable_estimator();
    estimator->set_name(class_name_);
    estimator->set_uid(uid_);
    estimator->set_type(operator_type_);

    // ----------------------------------------------------------------------
    // Set Ml Params
    // ----------------------------------------------------------------------
    ParamMap param_map = params_.copy();

    mutable_fit->mutable_params()->CopyFrom(to_ml_params(param_map));

    // ----------------------------------------------------------------------
    // Set input dataset (Relation)
    // ----------------------------------------------------------------------
    mutable_fit->mutable_dataset()->CopyFrom(input_df.plan().root());

    // ----------------------------------------------------------------------
    // Execute via gRPC
    // ----------------------------------------------------------------------
    grpc::ClientContext context;
    auto stream = input_df.stub()->ExecutePlan(&context, request);

    ExecutePlanResponse response;
    std::string model_ref_id;

    while (stream->Read(&response))
    {
        if (response.has_ml_command_result())
        {
            const auto& ml_result = response.ml_command_result();
            
            if (ml_result.has_operator_info() && ml_result.operator_info().has_obj_ref())
            {
                model_ref_id = ml_result.operator_info().obj_ref().id();
            }
        }
    }

    auto status = stream->Finish();

    if (!status.ok())
    {
        throw std::runtime_error("Word2Vec fit failed: " + status.error_message());
    }

    if (model_ref_id.empty())
    {
        throw std::runtime_error("Word2Vec fit returned no model reference");
    }

    ObjectRef obj_ref;
    obj_ref.set_id(model_ref_id);

    return Word2VecModel(input_df.stub(), input_df.session_id(), input_df.user_id(), obj_ref,
                         std::move(param_map));
}

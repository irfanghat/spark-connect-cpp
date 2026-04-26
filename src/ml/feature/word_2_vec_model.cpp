#include <stdexcept>

#include "word_2_vec_model.h"
#include "ml/param/param_map_proto.h"

DataFrame Word2VecModel::transform(const DataFrame& input_df) const
{
    if (!input_df.plan().has_root())
    {
        throw std::invalid_argument("Input DataFrame has no root relation");
    }

    Plan plan;
    auto* relation = plan.mutable_root();
    relation->mutable_common()->set_plan_id(input_df.plan().root().common().plan_id());

    auto* ml_transform = relation->mutable_ml_relation()->mutable_transform();

    ml_transform->mutable_obj_ref()->set_id(obj_ref_.id());

    ml_transform->mutable_input()->CopyFrom(input_df.plan().root());
    ml_transform->mutable_input()->mutable_common()->set_plan_id(
        input_df.plan().root().common().plan_id() + 1);

    ml_transform->mutable_params()->CopyFrom(to_ml_params(params_));

    return DataFrame(stub_, std::move(plan), session_id_, user_id_);
}

DataFrame Word2VecModel::getVectors() const
{
    Plan plan;
    auto* relation = plan.mutable_root();
    relation->mutable_common()->set_plan_id(1);

    auto* fetch = relation->mutable_ml_relation()->mutable_fetch();
    fetch->mutable_obj_ref()->set_id(obj_ref_.id());
    fetch->add_methods()->set_method("getVectors");

    return DataFrame(stub_, std::move(plan), session_id_, user_id_);
}

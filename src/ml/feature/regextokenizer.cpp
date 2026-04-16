#include "ml/feature/regextokenizer.h"
#include "ml/feature/tokenizer.h"
#include <uuid/uuid.h>

Relation RegexTokenizer::transform(const Relation &input_relation) 
{
    
    if (input_relation.rel_type_case() == spark::connect::Relation::REL_TYPE_NOT_SET) {
        throw std::invalid_argument("Input relation has no rel_type set");
    }

    Relation output_relation;

    output_relation.mutable_common()->set_plan_id(input_relation.common().plan_id());

    // Set Ml Operator
    
    auto* ml_relation = output_relation.mutable_ml_relation();
    auto* ml_transform = ml_relation->mutable_transform();
    auto* ml_operator = ml_transform->mutable_transformer();

    uuid_t uuid;
    uuid_generate(uuid);

    char ml_operator_uid[37];
    uuid_unparse(uuid, ml_operator_uid);

    ml_operator->set_uid("Tokenizer_" + std::string(ml_operator_uid));
    ml_operator->set_name(class_name_);
    ml_operator->set_type(operator_type_);

    // Set Ml Params

    auto* ml_param = ml_transform->mutable_params();
    auto* param_map = ml_param->mutable_params();

    (*param_map)["inputCol"].set_string(input_col_);
    (*param_map)["outputCol"].set_string(output_col_);
    (*param_map)["pattern"].set_string(pattern_);

    // Set input Relation
    
    auto* transform_input = ml_transform->mutable_input();

    transform_input->CopyFrom(input_relation);
    transform_input->mutable_common()->set_plan_id(input_relation.common().plan_id() + 1);

    return output_relation;
}

DataFrame RegexTokenizer::transform(const DataFrame &input_df)
{
    if (!input_df.plan().has_root()) {
        throw std::invalid_argument("Input DataFrame has no root relation");
    }
    
    auto transform_relation = transform(input_df.plan().root());

    Plan plan;
    plan.mutable_root()->Swap(&transform_relation);

    return DataFrame(input_df.stub(), std::move(plan), input_df.session_id(), input_df.user_id());
}
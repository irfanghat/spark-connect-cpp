#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <memory>

// gRPC includes
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
// #include <grpcpp/status.h>

// Spark Connect Protobuf generated headers
#include "spark/connect/base.pb.h"
#include "spark/connect/base.grpc.pb.h"
#include "spark/connect/relations.pb.h"
#include "spark/connect/expressions.pb.h"

// Namespaces
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

// Messages are defined in spark/connect/base.pb.h
using spark::connect::AnalyzePlanRequest;
using spark::connect::AnalyzePlanResponse;
using spark::connect::ConfigRequest;
using spark::connect::ConfigResponse;
using spark::connect::DataType;
using spark::connect::InterruptRequest;
using spark::connect::InterruptResponse;
using spark::connect::KeyValue;
using spark::connect::Plan;                // Plan is also in base.pb.h or relations.pb.h, but often in base.pb.h if it contains root Relation.
using spark::connect::SparkConnectService; // From base.grpc.pb.h
using spark::connect::StorageLevel;
using spark::connect::UserContext;

// Relation is defined in spark/connect/relations.pb.h
using spark::connect::Relation;

namespace Spark
{
    namespace Connect
    {
        namespace Client
        {

            /// <summary>
            /// Custom exception for Spark Connect errors.
            /// </summary>
            class SparkException : public std::runtime_error
            {
            public:
                explicit SparkException(const std::string &message) : std::runtime_error(message) {}
                explicit SparkException(const std::string &message, const Status &status)
                    : std::runtime_error(message + " (gRPC Code: " + std::to_string(status.error_code()) + ", Message: " + status.error_message() + ")") {}
            };

            /// <summary>
            /// A factory for creating SparkException from gRPC Status.
            /// </summary>
            class SparkExceptionFactory
            {
            public:
                static SparkException GetExceptionFromRpcStatus(const Status &status)
                {
                    if (!status.ok())
                    {
                        return SparkException("RPC call failed: " + status.error_message(), status);
                    }
                    return SparkException("An unknown RPC error occurred.");
                }
            };

            /// <summary>
            /// Represents a Spark Session, holding client and session-specific information.
            /// This is a simplified representation based on the C# SparkSession.
            /// </summary>
            struct SparkSession
            {
                std::string session_id;
                UserContext user_context;
                std::string client_type;
                std::unique_ptr<SparkConnectService::Stub> grpc_client;
                // In C++, gRPC metadata is typically passed via ClientContext.
                // This example simplifies by assuming metadata is set per call or managed elsewhere.
                // For specific headers that are *always* sent, we might populate ClientContext
                // within the SparkSession or have a factory for ClientContext.

                SparkSession(const std::string &host_port, const std::string &sess_id, const std::string &client_t)
                    : session_id(sess_id), client_type(client_t)
                {
                    // Initialize gRPC client stub
                    grpc_client = SparkConnectService::NewStub(
                        grpc::CreateChannel(host_port, grpc::InsecureChannelCredentials()));
                }
            };

            /// <summary>
            /// This is the class used to pass messages down the gRPC channel.
            /// </summary>
            class GrpcInternal
            {
            public:
                /// <summary>
                /// Explain
                /// </summary>
                /// <param name="client">The gRPC client stub.</param>
                /// <param name="sessionId">The session ID.</param>
                /// <param name="plan">The plan to explain.</param>
                /// <param name="headers">gRPC ClientContext for headers and call options.</param>
                /// <param name="userContext">The user context.</param>
                /// <param name="clientType">The client type string.</param>
                /// <param name="explainExtended">Whether to request extended explanation.</param>
                /// <param name="mode">Optional explain mode string (e.g., "simple", "extended", "codegen", "cost", "formatted").</param>
                /// <returns>The explanation string.</returns>
                static std::string Explain(
                    SparkConnectService::Stub *client,
                    const std::string &sessionId,
                    const Plan &plan,
                    ClientContext &headers, // headers object will be populated by the caller
                    const UserContext &userContext,
                    const std::string &clientType,
                    bool explainExtended,
                    const std::string &mode)
                {

                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::Explain *explain_op = analyze_request.mutable_explain();

                    // Default to Simple or Extended based on explainExtended flag
                    AnalyzePlanRequest::Explain::ExplainMode explain_mode_enum;
                    if (explainExtended)
                    {
                        explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_EXTENDED;
                    }
                    else
                    {
                        explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_SIMPLE;
                    }

                    // Override if a specific mode string is provided
                    if (!mode.empty())
                    {
                        if (mode == "simple")
                        {
                            explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_SIMPLE;
                        }
                        else if (mode == "extended")
                        {
                            explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_EXTENDED;
                        }
                        else if (mode == "codegen")
                        {
                            explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_CODEGEN;
                        }
                        else if (mode == "cost")
                        {
                            explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_COST;
                        }
                        else if (mode == "formatted")
                        {
                            explain_mode_enum = AnalyzePlanRequest::Explain::EXPLAIN_MODE_FORMATTED;
                        }
                        // To do: Add more modes if necessary based on Spark Connect protobuf definition
                    }

                    *explain_op->mutable_plan() = plan; // Copy the plan
                    explain_op->set_explain_mode(explain_mode_enum);

                    analyze_request.set_session_id(sessionId);
                    *analyze_request.mutable_user_context() = userContext; // Copy the user context
                    analyze_request.set_client_type(clientType);

                    AnalyzePlanResponse analyze_response;
                    Status status = client->AnalyzePlan(&headers, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    return analyze_response.explain().explain_string();
                }

                /// <summary>
                /// Persist
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="relation">The relation to persist.</param>
                /// <param name="storageLevel">The storage level to use.</param>
                /// <returns>The original relation (as per C# behavior).</returns>
                static Relation Persist(SparkSession &session, const Relation &relation, const StorageLevel &storageLevel)
                {
                    ClientContext context; // New context for this call
                    // To do: Add any common headers from session to context if needed
                    // for (const auto& pair : session.headers) { context.AddMetadata(pair.first, pair.second); }

                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::Persist *persist_op = analyze_request.mutable_persist();
                    *persist_op->mutable_relation() = relation;
                    *persist_op->mutable_storage_level() = storageLevel;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return relation; // Returns the original relation, not the response
                }

                /// <summary>
                /// Schema
                /// </summary>
                /// <param name="client">The gRPC client stub.</param>
                /// <param name="sessionId">The session ID.</param>
                /// <param name="plan">The plan to get schema for.</param>
                /// <param name="headers">gRPC ClientContext for headers and call options.</param>
                /// <param name="userContext">The user context.</param>
                /// <param name="clientType">The client type string.</param>
                /// <returns>The DataType representing the schema.</returns>
                static DataType Schema(
                    SparkConnectService::Stub *client,
                    const std::string &sessionId,
                    const Plan &plan,
                    ClientContext &headers,
                    const UserContext &userContext,
                    const std::string &clientType)
                {

                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::Schema *schema_op = analyze_request.mutable_schema();
                    *schema_op->mutable_plan() = plan;

                    analyze_request.set_session_id(sessionId);
                    *analyze_request.mutable_user_context() = userContext;
                    analyze_request.set_client_type(clientType);

                    AnalyzePlanResponse analyze_response;
                    Status status = client->AnalyzePlan(&headers, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.schema().schema(); // Note: 'schema_' because 'schema' is a keyword
                }

                /// <summary>
                /// What is the Spark Version you are connected to?
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <returns>The Spark version string.</returns>
                static std::string Version(SparkSession &session)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    analyze_request.mutable_spark_version(); // Create an empty SparkVersion message

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.spark_version().version();
                }

                /// <summary>
                /// Get a list of the input files
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="plan">The plan to analyze for input files.</param>
                /// <returns>A vector of input file paths.</returns>
                static std::vector<std::string> InputFiles(SparkSession &session, const Plan &plan)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::InputFiles *input_files_op = analyze_request.mutable_input_files();
                    *input_files_op->mutable_plan() = plan;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    std::vector<std::string> files;
                    for (const auto &file : analyze_response.input_files().files())
                    {
                        files.push_back(file);
                    }
                    return files;
                }

                /// <summary>
                /// Is it a local plan?
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="plan">The plan to check.</param>
                /// <returns>True if the plan is local, false otherwise.</returns>
                static bool IsLocal(SparkSession &session, const Plan &plan)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::IsLocal *is_local_op = analyze_request.mutable_is_local();
                    *is_local_op->mutable_plan() = plan;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.is_local().is_local(); // 'is_local_' to avoid keyword clash
                }

                /// <summary>
                /// Get the TreeString representation of a relation.
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="relation">The relation to get the tree string for.</param>
                /// <param name="level">Optional level for tree string depth.</param>
                /// <returns>The tree string.</returns>
                static std::string TreeString(SparkSession &session, const Relation &relation, int level = -1)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::TreeString *tree_string_op = analyze_request.mutable_tree_string();
                    *tree_string_op->mutable_plan()->mutable_root() = relation;

                    if (level != -1)
                    { // -1 indicates no specific level set
                        tree_string_op->set_level(level);
                    }

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.tree_string().tree_string(); // 'tree_string_' to avoid keyword clash
                }

                /// <summary>
                /// Create a semantic hash of the relation
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="relation">The relation to hash.</param>
                /// <returns>The semantic hash as an integer.</returns>
                static int SemanticHash(SparkSession &session, const Relation &relation)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::SemanticHash *semantic_hash_op = analyze_request.mutable_semantic_hash();
                    *semantic_hash_op->mutable_plan()->mutable_root() = relation;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.semantic_hash().result();
                }

                /// <summary>
                /// What is the storage level?
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="relation">The relation to check storage level for.</param>
                /// <returns>The StorageLevel.</returns>
                static StorageLevel GetStorageLevel(SparkSession &session, const Relation &relation)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::GetStorageLevel *get_storage_level_op = analyze_request.mutable_get_storage_level();
                    *get_storage_level_op->mutable_relation() = relation;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.get_storage_level().storage_level();
                }

                /// <summary>
                /// Is it a Streaming plan
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="plan">The plan to check.</param>
                /// <returns>True if the plan is streaming, false otherwise.</returns>
                static bool IsStreaming(SparkSession &session, const Plan &plan)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::IsStreaming *is_streaming_op = analyze_request.mutable_is_streaming();
                    *is_streaming_op->mutable_plan() = plan;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.is_streaming().is_streaming(); // 'is_streaming_' to avoid keyword clash
                }

                /// <summary>
                /// Same Semantics, uses AnalyzePlanRequest
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="target">The target relation.</param>
                /// <param name="other">The other relation to compare.</param>
                /// <returns>True if the relations have the same semantics, false otherwise.</returns>
                static bool SameSemantics(SparkSession &session, const Relation &target, const Relation &other)
                {
                    ClientContext context;
                    AnalyzePlanRequest analyze_request;
                    AnalyzePlanRequest::SameSemantics *same_semantics_op = analyze_request.mutable_same_semantics();
                    *same_semantics_op->mutable_target_plan()->mutable_root() = target;
                    *same_semantics_op->mutable_other_plan()->mutable_root() = other;

                    analyze_request.set_session_id(session.session_id);
                    *analyze_request.mutable_user_context() = session.user_context;
                    analyze_request.set_client_type(session.client_type);

                    AnalyzePlanResponse analyze_response;
                    Status status = session.grpc_client->AnalyzePlan(&context, analyze_request, &analyze_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }
                    return analyze_response.same_semantics().result();
                }

                /// <summary>
                /// Unset a config option
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="key">The config key to unset.</param>
                /// <exception cref="SparkException">Thrown if the RPC call fails.</exception>
                static void ExecUnSetConfigCommandResponse(SparkSession &session, const std::string &key)
                {
                    ClientContext context;
                    ConfigRequest config_request;
                    config_request.set_client_type(session.client_type);
                    config_request.set_session_id(session.session_id);
                    *config_request.mutable_user_context() = session.user_context;

                    ConfigRequest::Operation *operation = config_request.mutable_operation();
                    operation->mutable_unset()->add_keys(key);

                    ConfigResponse config_response;
                    Status status = session.grpc_client->Config(&context, config_request, &config_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    for (const auto &warning : config_response.warnings())
                    {
                        std::cerr << "Config::Warning: '" << warning << "'" << std::endl;
                    }
                }

                /// <summary>
                /// Set Config Item
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="options">A map of config key-value pairs to set.</param>
                /// <exception cref="SparkException">Thrown if the RPC call fails.</exception>
                static void ExecSetConfigCommandResponse(SparkSession &session, const std::map<std::string, std::string> &options)
                {
                    ClientContext context;
                    ConfigRequest config_request;
                    config_request.set_client_type(session.client_type);
                    config_request.set_session_id(session.session_id);
                    *config_request.mutable_user_context() = session.user_context;

                    ConfigRequest::Operation *operation = config_request.mutable_operation();
                    ConfigRequest::Set *set_op = operation->mutable_set();

                    for (const auto &pair : options)
                    {
                        KeyValue *kv = set_op->add_pairs();
                        kv->set_key(pair.first);
                        kv->set_value(pair.second);
                    }

                    ConfigResponse config_response;
                    Status status = session.grpc_client->Config(&context, config_request, &config_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    for (const auto &warning : config_response.warnings())
                    {
                        std::cerr << "Config::Warning: '" << warning << "'" << std::endl;
                    }
                }

                /// <summary>
                /// Get All Config Response
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <param name="prefix">Optional prefix to filter config keys.</param>
                /// <returns>A map of config key-value pairs.</returns>
                /// <exception cref="SparkException">Thrown if the RPC call fails.</exception>
                static std::map<std::string, std::string> ExecGetAllConfigCommandResponse(SparkSession &session, const std::string &prefix = "")
                {
                    ClientContext context;
                    ConfigRequest config_request;
                    config_request.set_client_type(session.client_type);
                    config_request.set_session_id(session.session_id);
                    *config_request.mutable_user_context() = session.user_context;

                    ConfigRequest::Operation *operation = config_request.mutable_operation();
                    ConfigRequest::GetAll *get_all_op = operation->mutable_get_all();

                    if (!prefix.empty())
                    {
                        get_all_op->set_prefix(prefix);
                    }

                    ConfigResponse config_response;
                    Status status = session.grpc_client->Config(&context, config_request, &config_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    for (const auto &warning : config_response.warnings())
                    {
                        std::cerr << "Config::Warning: '" << warning << "'" << std::endl;
                    }

                    std::map<std::string, std::string> items;
                    for (const auto &pair : config_response.pairs())
                    {
                        items[pair.key()] = pair.value();
                    }
                    return items;
                }

                /// <summary>
                /// Interrupt all operations of this session currently running on the connected server.
                /// </summary>
                /// <param name="session">The SparkSession object.</param>
                /// <returns>A list of interrupted operation IDs.</returns>
                /// <exception cref="SparkException">Thrown if the RPC call fails.</exception>
                static std::vector<std::string> InterruptAll(SparkSession &session)
                {
                    ClientContext context;
                    InterruptRequest interrupt_request;
                    interrupt_request.set_client_type(session.client_type);
                    interrupt_request.set_session_id(session.session_id);
                    *interrupt_request.mutable_user_context() = session.user_context;
                    interrupt_request.set_interrupt_type(InterruptRequest::INTERRUPT_TYPE_ALL);

                    InterruptResponse interrupt_response;
                    Status status = session.grpc_client->Interrupt(&context, interrupt_request, &interrupt_response);

                    if (!status.ok())
                    {
                        throw SparkExceptionFactory::GetExceptionFromRpcStatus(status);
                    }

                    std::vector<std::string> interrupted_ids;
                    for (const auto &id : interrupt_response.interrupted_ids())
                    {
                        interrupted_ids.push_back(id);
                    }
                    return interrupted_ids;
                }
            };

        } // namespace Client
    } // namespace Connect
} // namespace Spark
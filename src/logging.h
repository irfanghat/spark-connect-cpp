#pragma once

#include <iostream>
#include <string>
#include <ctime>
#include <iomanip>
#include <unistd.h>

namespace spark::sql::logging
{
    enum class LogLevel
    {
        DEBUG,
        INFO,
        WARN,
        ERROR
    };

    class Logger
    {
    public:
        static void set_level(LogLevel level) { get_instance().current_level_ = level; }

        static void log(LogLevel level, const std::string &tag, const std::string &message)
        {
            if (level < get_instance().current_level_)
                return;

            bool is_error = (level == LogLevel::ERROR);
            std::ostream &os = is_error ? std::cerr : std::cout;

            // ------------------------------------------------
            // Only use colors if the output is a terminal
            // ------------------------------------------------
            bool use_color = isatty(is_error ? STDERR_FILENO : STDOUT_FILENO);

            if (use_color)
                os << level_to_color(level);

            os << "[" << current_time() << "] "
               << "[" << level_to_string(level) << "] "
               << "[" << tag << "] "
               << message;

            if (use_color)
                os << "\033[0m"; // Reset to default

            os << std::endl;
        }

    private:
        LogLevel current_level_ = LogLevel::INFO;

        static Logger &get_instance()
        {
            static Logger instance;
            return instance;
        }

        static std::string level_to_color(LogLevel level)
        {
            switch (level)
            {
            case LogLevel::DEBUG:
                return "\033[36m"; // Cyan
            case LogLevel::INFO:
                return "\033[32m"; // Green
            case LogLevel::WARN:
                return "\033[33m"; // Yellow
            case LogLevel::ERROR:
                return "\033[31m"; // Red
            default:
                return "\033[0m";
            }
        }

        static std::string level_to_string(LogLevel level)
        {
            switch (level)
            {
            case LogLevel::DEBUG:
                return "DEBUG";
            case LogLevel::INFO:
                return "INFO ";
            case LogLevel::WARN:
                return "WARN ";
            case LogLevel::ERROR:
                return "ERROR";
            default:
                return "UNKNOWN";
            }
        }

        static std::string current_time()
        {
            std::time_t now = std::time(nullptr);
            char buf[20];
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
            return std::string(buf);
        }
    };

#define SPARK_LOG_INFO(tag, msg) spark::sql::logging::Logger::log(spark::sql::logging::LogLevel::INFO, tag, msg)
#define SPARK_LOG_ERROR(tag, msg) spark::sql::logging::Logger::log(spark::sql::logging::LogLevel::ERROR, tag, msg)
#define SPARK_LOG_DEBUG(tag, msg) spark::sql::logging::Logger::log(spark::sql::logging::LogLevel::DEBUG, tag, msg)
#define SPARK_LOG_WARN(tag, msg) spark::sql::logging::Logger::log(spark::sql::logging::LogLevel::WARN, tag, msg)

}
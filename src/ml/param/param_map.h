#pragma once

#include <map>
#include <optional>
#include <string>
#include <variant>

using ParamValue = std::variant<std::string, int, int64_t, double, bool>;

class ParamMap
{
  public:
    ParamMap() = default;
    ~ParamMap() = default;

    ParamMap& put(const std::string& key, const std::string& value);
    ParamMap& put(const std::string& key, int value);
    ParamMap& put(const std::string& key, int64_t value);
    ParamMap& put(const std::string& key, double value);
    ParamMap& put(const std::string& key, bool value);

    std::optional<ParamValue> get(const std::string& key) const;
    bool contains(const std::string& key) const;
    std::optional<ParamValue> remove(const std::string& key);
    int size() const;
    bool is_empty() const;
    ParamMap copy() const;

    const std::map<std::string, ParamValue>& entries() const;

  private:
    std::map<std::string, ParamValue> params_;
};

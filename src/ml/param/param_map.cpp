#include "param_map.h"

ParamMap& ParamMap::put(const std::string& key, const std::string& value)
{
    params_[key] = value;
    return *this;
}

ParamMap& ParamMap::put(const std::string& key, int value)
{
    params_[key] = value;
    return *this;
}

ParamMap& ParamMap::put(const std::string& key, int64_t value)
{
    params_[key] = value;
    return *this;
}

ParamMap& ParamMap::put(const std::string& key, double value)
{
    params_[key] = value;
    return *this;
}

ParamMap& ParamMap::put(const std::string& key, bool value)
{
    params_[key] = value;
    return *this;
}

std::optional<ParamValue> ParamMap::get(const std::string& key) const
{
    auto it = params_.find(key);
    if (it == params_.end())
        return std::nullopt;
    return it->second;
}

bool ParamMap::contains(const std::string& key) const
{
    return params_.find(key) != params_.end();
}

std::optional<ParamValue> ParamMap::remove(const std::string& key)
{
    auto it = params_.find(key);
    if (it == params_.end())
        return std::nullopt;
    auto value = it->second;
    params_.erase(it);
    return value;
}

int ParamMap::size() const
{
    return static_cast<int>(params_.size());
}

bool ParamMap::is_empty() const
{
    return params_.empty();
}

ParamMap ParamMap::copy() const
{
    return *this;
}

const std::map<std::string, ParamValue>& ParamMap::entries() const
{
    return params_;
}

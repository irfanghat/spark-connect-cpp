#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

template <typename T>
class Param
{
    public:
        // ------------------------------------------------------------------
        // Types
        // ------------------------------------------------------------------

        using ValidatorFn = std::function<bool(const T&)>;

        // ------------------------------------------------------------------
        // Constructors
        // ------------------------------------------------------------------

        Param(const std::string& parent, const std::string& name, const std::string& doc, ValidatorFn is_valid)
            : parent_(std::move(parent)), name_(std::move(name)), doc_(std::move(doc)), is_valid_(std::move(is_valid)) 
        {
            if (name_.empty()) {
                throw std::invalid_argument("Param name cannot be empty");
            }
        }

        Param(const std::string& parent, const std::string& name, const std::string& doc)
            : Param(std::move(parent), std::move(name), std::move(doc), [](const T&) { return true; }) {}

        // ------------------------------------------------------------------
        // Accessors
        // ------------------------------------------------------------------

        const std::string& parent() const
        {
            return parent_;
        }

        const std::string& name() const
        {
            return name_;
        }

        const std::string& doc() const
        {
            return doc_;
        }

        ValidatorFn is_valid() const
        {
            return is_valid_;
        }

        // ------------------------------------------------------------------
        // Validation
        // ------------------------------------------------------------------

        bool is_valid(const T& value) const
        {
            return is_valid_(value);
        }

        // ------------------------------------------------------------------
        // Convenience
        // ------------------------------------------------------------------

        std::string key() const
        {
            return parent_ + "__" + name_;
        }

        std::string toString() const
        {
            return key() + ": " + doc_;
        }

        friend bool operator == (const Param<T>& a, const Param<T>& b)
        {
            return a.key() == b.key();
        }

        friend bool operator < (const Param<T>& a, const Param<T>& b)
        {
            return std::tie(a.parent(), a.name()) < std::tie(b.parent(), b.name());
        }

    private:
        std::string parent_;
        std::string name_;
        std::string doc_;

        ValidatorFn is_valid_ = nullptr;
};

inline auto param_is_valid_number = [](const double& v) -> bool {
    return std::isfinite(v);
};

inline auto param_is_valid_string = [](const std::string& s) -> bool {
    return std::any_of(s.begin(), s.end(), [](unsigned char c) { return !std::isspace(c); });
};

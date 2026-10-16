#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <functional>
#include <stdexcept>
#include <string>

template <typename T> class Param
{
  public:
    using ValidatorFn = std::function<bool(T const&)>;

    Param(std::string const& parent, std::string const& name,
          std::string const& doc, ValidatorFn is_valid)
        : parent_(std::move(parent))
        , name_(std::move(name))
        , doc_(std::move(doc))
        , is_valid_(std::move(is_valid))
    {
        if (name_.empty())
        {
            throw std::invalid_argument("Param name cannot be empty");
        }
    }

    Param(std::string const& parent, std::string const& name,
          std::string const& doc)
        : Param(std::move(parent), std::move(name), std::move(doc),
                [](T const&) { return true; })
    {
    }

    std::string const&
    parent() const
    {
        return parent_;
    }

    std::string const&
    name() const
    {
        return name_;
    }

    std::string const&
    doc() const
    {
        return doc_;
    }

    ValidatorFn
    is_valid() const
    {
        return is_valid_;
    }

    bool
    is_valid(T const& value) const
    {
        return is_valid_(value);
    }

    std::string
    key() const
    {
        return parent_ + "__" + name_;
    }

    std::string
    toString() const
    {
        return key() + ": " + doc_;
    }

    friend bool
    operator==(Param<T> const& a, Param<T> const& b)
    {
        return a.key() == b.key();
    }

    friend bool
    operator<(Param<T> const& a, Param<T> const& b)
    {
        return std::tie(a.parent(), a.name()) < std::tie(b.parent(), b.name());
    }

  private:
    std::string parent_;
    std::string name_;
    std::string doc_;

    ValidatorFn is_valid_ = nullptr;
};

inline auto param_is_valid_number = [](double const& v) -> bool {
    return std::isfinite(v);
};

inline auto param_is_valid_string = [](std::string const& s) -> bool {
    return std::any_of(s.begin(), s.end(),
                       [](unsigned char c) { return !std::isspace(c); });
};

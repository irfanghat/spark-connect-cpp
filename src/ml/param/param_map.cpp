#include <sstream>

#include "param_map.h"

std::string ParamMap::toString() const
{
    std::ostringstream out;
    
    out << "{";

    bool first = true;

    for (const auto& [key, value] : map_)
    {
        if (!first)
            out << ", ";

        first = false;

        out << key << "=";

        std::visit(
            [&](auto&& v)
            {
                using V = std::decay_t<decltype(v)>;

                if constexpr (std::is_same_v<V, std::string>)
                    out << '"' << v << '"';

                else if constexpr (std::is_same_v<V, bool>)
                    out << (v ? "true" : "false");

                else if constexpr (std::is_same_v<V, std::vector<std::string>>)
                {
                    out << '[';

                    bool first_element = true;

                    for (const auto& element : v)
                    {
                        if (!first_element)
                            out << ", ";

                        first_element = false;

                        out << '"' << element << '"';
                    }

                    out << ']';
                }

                else
                    out << v;
            },
            value
        );
    }

    out << "}";

    return out.str();
}

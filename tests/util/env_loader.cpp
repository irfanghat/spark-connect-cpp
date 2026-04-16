#include "env_loader.h"
#include <algorithm>
#include <fstream>
#include <sstream>

static inline std::string trim(const std::string& s)
{
    auto start = s.find_first_not_of(" \t\r\n");
    auto end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos)
        return "";
    return s.substr(start, end - start + 1);
}

void load_env(const std::string& path)
{
    std::ifstream file(path);

    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open .env file: " + path);
    }

    std::string line;

    while (std::getline(file, line))
    {
        line = trim(line);

        // skip empty lines and comments
        if (line.empty() || line[0] == '#')
            continue;

        auto pos = line.find('=');
        if (pos == std::string::npos)
            continue;

        std::string key = trim(line.substr(0, pos));
        std::string val = trim(line.substr(pos + 1));

        // remove optional quotes
        if (!val.empty() && val.front() == '"' && val.back() == '"')
        {
            val = val.substr(1, val.size() - 2);
        }

        setenv(key.c_str(), val.c_str(), 1);
    }
}
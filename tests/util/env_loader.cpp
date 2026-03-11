#include "env_loader.h"

void load_env(const std::string& path)
{
    std::ifstream file(path);
    std::string line;

    while (std::getline(file, line))
    {
        auto pos = line.find('=');
        if (pos != std::string::npos)
        {
            auto key = line.substr(0, pos);
            auto val = line.substr(pos + 1);

            // overwrite existing environment variables
            setenv(key.c_str(), val.c_str(), 1);
        }
    }
}

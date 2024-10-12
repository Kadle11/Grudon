#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

template<typename T>
std::string fmt_array(const T& arr) {
    std::stringstream ss;
    for (auto& elem : arr) {
        ss << elem << " ";
    }
    return ss.str();
}

#endif  // LOGGER_HPP
#pragma once

#include <cstdint>
#include <optional>
#include <variant>

template <typename... Records>
struct MergedScanner {
    uint64_t produced = 0;
    virtual ~MergedScanner() = default;
    virtual void reset() = 0;

    virtual std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> next() = 0;

    virtual std::optional<std::pair<std::variant<typename Records::Key...>, std::variant<Records...>>> current() = 0;
};
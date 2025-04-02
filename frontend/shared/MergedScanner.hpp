#pragma once

#include <optional>
#include <variant>

template <typename JK, typename... Records>
struct MergedScannerInterface {
    virtual ~MergedScannerInterface() = default;

    virtual void reset() = 0;

    virtual std::optional<std::variant<Records...>> nextRecord() = 0;

    virtual bool seek(const JK& jk) = 0;

    virtual void scanJoin() = 0;
};
#pragma once
#include <variant> // IWYU pragma: keep
#include <tuple> // IWYU pragma: keep

template <class... Ts>
struct overloaded : Ts... {
   using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
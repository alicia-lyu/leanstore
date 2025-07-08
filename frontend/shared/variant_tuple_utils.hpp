#pragma once
#include <tuple>    // IWYU pragma: keep
#include <variant>  // IWYU pragma: keep

template <class... Ts>
struct overloaded : Ts... {
   using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template <typename Tuple, typename Func>
static void for_each(Tuple& tup, Func func)
{
   std::apply([&](auto&... elems) { (..., func(elems)); }, tup);
}
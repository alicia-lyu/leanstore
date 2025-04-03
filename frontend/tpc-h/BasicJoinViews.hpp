#pragma once

#include "ViewsTemplate.hpp"

struct PPsL_JK {
    Integer l_partkey;
    Integer suppkey;

    PPsL_JK() = default;
    PPsL_JK(Integer partkey, Integer partsuppkey): l_partkey(partkey), suppkey(partsuppkey) {}
    PPsL_JK(const PPsL_JK& jk): l_partkey(jk.l_partkey), suppkey(jk.suppkey) {}

    static PPsL_JK max() {
        return PPsL_JK({std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()});
    }

    static unsigned keyfold(uint8_t* out, const PPsL_JK& key) {
        unsigned pos = 0;
        pos += fold(out + pos, key.l_partkey); // TODO: Only fold the first field for part_t
        pos += fold(out + pos, key.suppkey);
        return pos;
    };

    static unsigned keyunfold(const uint8_t* in, PPsL_JK& key) {
        unsigned pos = 0;
        pos += unfold(in + pos, key.l_partkey);
        pos += unfold(in + pos, key.suppkey);
        return pos;
    };

    static constexpr unsigned maxFoldLength() {
        return sizeof(Integer) + sizeof(Integer);
    }

    friend std::ostream& operator<<(std::ostream& os, const PPsL_JK& jk) {
        os << "PPsL_JK(" << jk.l_partkey << ", " << jk.suppkey << ")";
        return os;
    }

    auto operator<=>(const PPsL_JK&) const = default;

    friend int operator%(const PPsL_JK& jk, const int& n) {
        return (jk.suppkey + jk.l_partkey) % n;
    }

    int match(const PPsL_JK& other) const {
        // {0, 0} cannot be used as wildcard
        if (*this == PPsL_JK{} && other == PPsL_JK{})
            return 0;
        else if (*this == PPsL_JK{})
            return -1;
        else if (other == PPsL_JK{})
            return 1;

        if (l_partkey != 0 && other.l_partkey != 0 && l_partkey != other.l_partkey)
            return l_partkey - other.l_partkey;
        if (suppkey != 0 && other.suppkey != 0)
            return suppkey - other.suppkey;
        return 0;
    }
};

struct merged_part_t : public merged_t<13, part_t, PPsL_JK, false> {
    using merged_t::merged_t;

    static PPsL_JK getJK(const PPsL_JK& jk) {
        return {jk.l_partkey, 0};
    }
};

using merged_partsupp_t = merged_t<13, partsupp_t, PPsL_JK, false>;

using merged_lineitem_t = merged_t<13, lineitem_t, PPsL_JK, true>;

struct sorted_lineitem_t : public merged_t<14, lineitem_t, PPsL_JK, true> {
    using merged_t::merged_t;
 
    operator merged_lineitem_t() const {
        return merged_lineitem_t{this->payload};
    }
 
    struct Key: public merged_t::Key {
        using merged_t::Key::Key;
        operator merged_lineitem_t::Key() const {
            return merged_lineitem_t::Key{this->jk, this->pk};
        }
    };
 };

struct joinedPPs_t : public joined_t<11, PPsL_JK, part_t, partsupp_t> {
    using joined_t::joined_t;

    joinedPPs_t(merged_part_t p, merged_partsupp_t ps): joined_t(std::make_tuple(p.payload, ps.payload)) {}

    struct Key: public joined_t::Key {
        Key() = default;
        Key(const joined_t::Key& k): joined_t::Key(k) {}
        Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk): joined_t::Key({psk.jk, std::tuple_cat(std::make_tuple(pk.pk), std::make_tuple(psk.pk))}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk): joined_t::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk)}) {}
    };
};

struct joinedPPsL_t : public joined_t<12, PPsL_JK, part_t, partsupp_t, lineitem_t> {
    using joined_t::joined_t;
    joinedPPsL_t(merged_part_t p, merged_partsupp_t ps, merged_lineitem_t l): joined_t(std::make_tuple(p.payload, ps.payload, l.payload)) {}

    joinedPPsL_t(part_t p, partsupp_t ps, merged_lineitem_t l): joined_t(std::make_tuple(p, ps, l.payload)) {}

    joinedPPsL_t(joinedPPs_t j, merged_lineitem_t l): joined_t(std::tuple_cat(j.payloads, std::make_tuple(l.payload))) {}

    struct Key: public joined_t::Key {
        Key() = default;
        Key(const joined_t::Key& k): joined_t::Key(k) {}
        Key(const joinedPPs_t::Key& j1k, const merged_lineitem_t::Key& lk): joined_t::Key({j1k.jk, std::tuple_cat(j1k.keys, std::make_tuple(lk.pk))}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk, const lineitem_t::Key& lk): joined_t::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk, lk)}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk, const merged_lineitem_t::Key& lk): joined_t::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk, lk.pk)}) {}
        Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk, const merged_lineitem_t::Key& lk): joined_t::Key({PPsL_JK{pk.jk.l_partkey, psk.jk.suppkey}, std::make_tuple(pk.pk, psk.pk, lk.pk)}) {}

    };
};


template <>
struct JKBuilder<PPsL_JK> {

    static PPsL_JK inline create(const part_t::Key& k, const part_t&){
        return PPsL_JK(k.p_partkey, 0);
    }

    static PPsL_JK inline create(const partsupp_t::Key& k, const partsupp_t&) {
        return PPsL_JK(k.ps_partkey, k.ps_suppkey);
    }

    static PPsL_JK inline create(const lineitem_t::Key&, const lineitem_t& v) {
        return PPsL_JK(v.l_partkey, v.l_suppkey);
    }

    static PPsL_JK inline create(const joinedPPs_t::Key& k, const joinedPPs_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const joinedPPsL_t::Key& k, const joinedPPsL_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_part_t::Key& k, const merged_part_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_partsupp_t::Key& k, const merged_partsupp_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_lineitem_t::Key& k, const merged_lineitem_t&) {
        return PPsL_JK(k.jk);
    }

    template <typename Record>
    static PPsL_JK inline get(const PPsL_JK& k) {
        return k;
    }
};

template <>
inline PPsL_JK JKBuilder<PPsL_JK>::get<part_t>(const PPsL_JK& jk) {
    return {jk.l_partkey, 0};
}

template <>
inline PPsL_JK JKBuilder<PPsL_JK>::get<merged_part_t>(const PPsL_JK& jk) {
    return {jk.l_partkey, 0};
}
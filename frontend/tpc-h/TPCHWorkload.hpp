#pragma once
#include "Tables.hpp"
#include <gflags/gflags.h>
#include "leanstore/utils/RandomGenerator.hpp"


DEFINE_int32(tpch_scale_factor, 1, "TPC-H scale factor");


template <template <typename> class AdapterType>
class TPCHWorkload
{
   private:
    AdapterType<part_t>& part;
    AdapterType<supplier_t>& supplier;
    AdapterType<partsupp_t>& partsupp;
    AdapterType<customer_t>& customer;
    AdapterType<orders_t>& orders;
    AdapterType<lineitem_t>& lineitem;
    AdapterType<nation_t>& nation;
    AdapterType<region_t>& region;

   public:
    TPCHWorkload(AdapterType<part_t>& p,
                 AdapterType<supplier_t>& s,
                 AdapterType<partsupp_t>& ps,
                 AdapterType<customer_t>& c,
                 AdapterType<orders_t>& o,
                 AdapterType<lineitem_t>& l,
                 AdapterType<nation_t>& n,
                 AdapterType<region_t>& r)
        : part(p), supplier(s), partsupp(ps), customer(c), orders(o), lineitem(l), nation(n), region(r)
    {
    }

    private:
        static constexpr Integer PART_SCALE = 200000;
        static constexpr Integer SUPPLIER_SCALE = 10000;
        static constexpr Integer CUSTOMER_SCALE = 150000;
        static constexpr Integer ORDERS_SCALE = 1500000;
        static constexpr Integer LINEITEM_SCALE = 6000000;
        static constexpr Integer PARTSUPP_SCALE = 800000;
        static constexpr Integer NATION_COUNT = 25;
        static constexpr Integer REGION_COUNT = 5;
        
        // [0, n)
        Integer rnd(Integer n) { return leanstore::utils::RandomGenerator::getRand(0, n); }
        // [low, high]
        Integer urand(Integer low, Integer high) { return rnd(high - low + 1) + low; }

        inline Integer getPartID()
        {
            return urand(1, PART_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getSupplierID()
        {
            return urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getCustomerID()
        {
            return urand(1, CUSTOMER_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getOrderID()
        {
            return urand(1, ORDERS_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getLineItemID()
        {
            return urand(1, LINEITEM_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getPartsuppID()
        {
            return urand(1, PARTSUPP_SCALE * FLAGS_tpch_scale_factor);
        }

    public:

        void basicJoin();

        void basicGroup();

        void basicJoinGroup();

        void prepare();

        void loadPart();

        void loadSupplier();

        void loadPartsupp();

        void loadCustomer();

        void loadOrders();

        void loadLineitem();

        void loadNation();

        void loadRegion();

};







     


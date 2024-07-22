#pragma once
#include <functional>
#include <variant>
#include "leanstore/KVInterface.hpp"

template <class... Records>
struct RecordVariant {
   using Key = std::variant<typename Records::Key...>;
   using Value = std::variant<Records...>;
};

template <class... Records>
class MergedAdapter
{
   using Record = RecordVariant<Records...>;

  public:
   virtual void scan(const typename Record::Key& key,
                     const std::function<bool(const typename Record::Key&, const typename Record::Value&)>& found_record_cb,
                     std::function<void()> reset_if_scan_failed_cb) = 0;

   virtual void insert(const typename Record::Key& key, const typename Record::Value& record) = 0;

   virtual void lookup(const typename Record::Key& key, const std::function<void(const typename Record::Value&)>& callback) = 0;

   virtual void update(const typename Record::Key& key,
                       const std::function<void(typename Record::Value&)>& update_the_record_in_place_cb,
                       leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) = 0;

   virtual bool erase(const typename Record::Key& key) = 0;
};
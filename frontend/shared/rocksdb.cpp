#include <rocksdb/db.h>
#include "RocksDB.hpp"

using namespace rocksdb;

// Force the compiler to include type information for rocksdb::DB
void ensureTypeInfo() {
    [[maybe_unused]] const std::type_info& ti = typeid(DB);
}
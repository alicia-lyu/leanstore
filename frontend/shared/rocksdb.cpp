#include <rocksdb/db.h>

using namespace rocksdb;

// Force the compiler to include type information for rocksdb::DB
void ensureTypeInfo() {
    [[maybe_unused]] const std::type_info& ti = typeid(DB);
}
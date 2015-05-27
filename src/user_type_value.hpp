/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __CASS_USER_TYPE_VALUE_HPP_INCLUDED__
#define __CASS_USER_TYPE_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "collection.hpp"
#include "data_type.hpp"
#include "encode.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

namespace cass {

class UserTypeValue {
public:
  UserTypeValue(const SharedRefPtr<UserType>& user_type)
    : user_type_(user_type) {
    items_.resize(user_type->fields().size());
  }

  const SharedRefPtr<UserType>& user_type() const { return user_type_; }
  const BufferVec& items() const { return items_; }

#define CHECK_INDEX_AND_TYPE(Index, Value) do { \
  if (Index >= items_.size()) {                 \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;  \
  }                                             \
  CassError rc = validate_type(Index, Value);   \
  if (rc != CASS_OK) return rc;                 \
} while(0)

#define SET_TYPE(DeclType) \
  CassError set(size_t index, const DeclType value) { \
    CHECK_INDEX_AND_TYPE(index, value); \
    items_[index] = cass::encode_with_length(value); \
    return CASS_OK; \
  }

  SET_TYPE(cass_int32_t)
  SET_TYPE(cass_int64_t)
  SET_TYPE(cass_float_t)
  SET_TYPE(cass_double_t)
  SET_TYPE(bool)
  SET_TYPE(CassString)
  SET_TYPE(CassBytes)
  SET_TYPE(CassUuid)
  SET_TYPE(CassInet)
  SET_TYPE(CassDecimal)

#undef SET_TYPE

  CassError set(size_t index, const Collection* value) {
    CHECK_INDEX_AND_TYPE(index, value);
    if (value->type() == CASS_COLLECTION_TYPE_MAP &&
        value->items().size() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    items_[index] = value->encode_with_length();
    return CASS_OK;
  }

  CassError set(size_t index, const UserTypeValue* value) {
    CHECK_INDEX_AND_TYPE(index, value);
    items_[index] = value->encode_with_length();
    return CASS_OK;
  }

#undef CHECK_INDEX_AND_TYPE

  Buffer encode_with_length() const {
    size_t items_size = get_items_size();

    Buffer buf(sizeof(int32_t) + items_size);

    size_t pos = buf.encode_int32(0, items_size);
    for (BufferVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      pos = buf.copy(pos, i->data(), i->size());
    }

    return buf;
  }

private:
  size_t get_items_size() const {
    size_t size = 0;
    for (BufferVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      size += i->size();
    }
    return size;
  }

  template <class T>
  CassError validate_type(size_t index, const T value) {
    IsValidDataType<T> is_valid_type;
    if (!is_valid_type(value, user_type_->fields()[index].type)) {
      return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    return CASS_OK;
  }

private:
  SharedRefPtr<UserType> user_type_;
  BufferVec items_;
};

} // namespace cass

#endif

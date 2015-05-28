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
#include "data_type.hpp"
#include "encode.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

#define CASS_USER_TYPE_CHECK_INDEX_AND_TYPE(Index, Value) do { \
  CassError rc = check(Index, Value);                          \
  if (rc != CASS_OK) return rc;                                \
} while(0)

namespace cass {

class Collection;

class UserTypeValue {
public:
  UserTypeValue(const SharedRefPtr<UserType>& user_type)
    : user_type_(user_type) {
    items_.resize(user_type->fields().size());
  }

  const SharedRefPtr<UserType>& user_type() const { return user_type_; }
  const BufferVec& items() const { return items_; }

#define SET_TYPE(Type)                                 \
  CassError set(size_t index, const Type value) {      \
    CASS_USER_TYPE_CHECK_INDEX_AND_TYPE(index, value); \
    items_[index] = cass::encode_with_length(value);   \
    return CASS_OK;                                    \
  }

  SET_TYPE(CassNull)
  SET_TYPE(cass_int32_t)
  SET_TYPE(cass_int64_t)
  SET_TYPE(cass_float_t)
  SET_TYPE(cass_double_t)
  SET_TYPE(cass_bool_t)
  SET_TYPE(CassString)
  SET_TYPE(CassBytes)
  SET_TYPE(CassUuid)
  SET_TYPE(CassInet)
  SET_TYPE(CassDecimal)

#undef SET_TYPE

  CassError set(size_t index, const Collection* value);
  CassError set(size_t index, const UserTypeValue* value);

  Buffer encode() const;
  Buffer encode_with_length() const;

private:
  size_t get_items_size() const;

  void encode_items(size_t pos, Buffer* buf) const;

  template <class T>
  CassError check(size_t index, const T value) {
    if (index >= items_.size()) {                 \
      return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;  \
    }                                             \
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

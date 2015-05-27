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

#ifndef __CASS_COLLECTION_HPP_INCLUDED__
#define __CASS_COLLECTION_HPP_INCLUDED__

#include "cassandra.h"
#include "data_type.hpp"
#include "encode.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"
#include "types.hpp"

namespace cass {

class Collection {
public:
  Collection(int protocol_version,
             CassCollectionType type,
             size_t item_count)
    : type_(type)
    , protocol_version_(protocol_version) {
    items_.reserve(item_count);
  }

  CassCollectionType type() const { return type_; }
  const BufferVec& items() const { return items_; }

#define APPEND_TYPE(DeclType)           \
  void append(const DeclType value) {   \
    items_.push_back(cass::encode(value)); \
  }

  APPEND_TYPE(cass_int32_t)
  APPEND_TYPE(cass_int64_t)
  APPEND_TYPE(cass_float_t)
  APPEND_TYPE(cass_double_t)
  APPEND_TYPE(bool)
  APPEND_TYPE(CassString)
  APPEND_TYPE(CassBytes)
  APPEND_TYPE(CassUuid)
  APPEND_TYPE(CassInet)
  APPEND_TYPE(CassDecimal)

#undef APPEND_TYPE

  Buffer encode_items() const {
    Buffer buf(get_values_size());
    encode_items(0, &buf);
    return buf;
  }

  Buffer encode_with_length() const {
    size_t value_size = get_size() + get_values_size();

    Buffer buf(sizeof(int32_t) + value_size);

    size_t pos = buf.encode_int32(0, value_size);
    pos = encode_size(pos, &buf, get_count());
    encode_items(pos, &buf);

    return buf;
  }

private:
  int32_t get_count() const {
    return ((type_ == CASS_COLLECTION_TYPE_MAP) ? items_.size() / 2 : items_.size());
  }

  size_t get_size() const {
    return (protocol_version_ >= 3) ? sizeof(int32_t) : sizeof(uint16_t);
  }

  size_t encode_size(size_t pos, Buffer* buf, int32_t size) const {
    if (protocol_version_ >= 3) {
      pos = buf->encode_int32(pos, size);
    } else {
      pos = buf->encode_uint16(pos, size);
    }
    return pos;
  }

  size_t get_values_size() const {
    size_t size = 0;
    for (BufferVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      size += get_size();
      size += i->size();
    }
    return size;
  }

  void encode_items(size_t pos, Buffer* buf) const {
    for (BufferVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      pos = encode_size(pos, buf, i->size());
      pos = buf->copy(pos, i->data(), i->size());
    }
  }

private:
  CassCollectionType type_;
  int protocol_version_;
  BufferVec items_;
};

} // namespace cass

#endif


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

#ifndef __CASS_INPUT_VALUE_HPP_INCLUDED__
#define __CASS_INPUT_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "data_type.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"

#include <vector>

namespace cass {

struct CassNull {};

struct CassBytes {
  CassBytes(const cass_byte_t* data, size_t size)
    : data(data), size(size) { }
  const cass_byte_t* data;
  size_t size;
};

struct CassString {
  CassString(const char* data, size_t length)
    : data(data), length(length) { }
  const char* data;
  size_t length;
};

struct CassDecimal {
  CassDecimal(const cass_byte_t* varint,
              size_t varint_size,
              int scale)
    : varint(varint)
    , varint_size(varint_size)
    , scale(scale) { }
  const cass_byte_t* varint;
  size_t varint_size;
  cass_int32_t scale;
};

struct CassCustom {
  uint8_t** output;
  size_t output_size;
};

class InputValue : public RefCounted<InputValue> {
public:
  InputValue(int type)
    : type_(type) { }

  int type() const { return type_; }

  bool is_collection() const {
    return type_ == CASS_VALUE_TYPE_LIST
        || type_ == CASS_VALUE_TYPE_SET
        || type_ == CASS_VALUE_TYPE_MAP;
  }

  bool is_routable() const {
    // TODO:
    return false;
  }

  virtual ~InputValue() { }
  virtual size_t get_size() const = 0;
  virtual Buffer encode() const = 0;
  virtual size_t copy_encoded(size_t offset, Buffer* buf) const = 0;

protected:
  int type_;
};

typedef std::vector<SharedRefPtr<const InputValue> > InputValueVec;

class NullInputValue : public InputValue {
public:
  NullInputValue()
    : InputValue(CASS_VALUE_TYPE_UNKNOWN)
    , buf_(sizeof(int32_t)) {
    buf_.encode_int32(0, -1); // [bytes] "null"
  }

  virtual size_t get_size() const {
    return buf_.size();
  }

  virtual Buffer encode() const {
    return buf_;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, buf_.data(), buf_.size());
  }

private:
  Buffer buf_;
};

class CustomInputValue : public InputValue {
public:
  CustomInputValue(size_t len)
    : InputValue(CASS_VALUE_TYPE_CUSTOM)
    , buf_(sizeof(int32_t) + len) {
    buf_.encode_int32(0, len);
  }

  virtual size_t get_size() const {
    return buf_.size();
  }

  virtual Buffer encode() const {
    return buf_;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, buf_.data(), buf_.size());
  }

  char* data() {
    return buf_.data() + sizeof(int32_t);
  }

private:
  Buffer buf_;
};

class SimpleInputValue : public InputValue {
public:
  explicit SimpleInputValue(int32_t value)
    : InputValue(CASS_VALUE_TYPE_INT)
    , buf_(sizeof(int32_t) + sizeof(int32_t)) {
    size_t pos = buf_.encode_int32(0, sizeof(int32_t));
    buf_.encode_int32(pos, value);
  }

  explicit SimpleInputValue(int64_t value)
    : InputValue(CASS_VALUE_TYPE_BIGINT)
    , buf_(sizeof(int32_t) + sizeof(int64_t)) {
    size_t pos = buf_.encode_int32(0, sizeof(int64_t));
    buf_.encode_int64(pos, value);
  }

  explicit SimpleInputValue(float value)
    : InputValue(CASS_VALUE_TYPE_FLOAT)
    , buf_(sizeof(int32_t) + sizeof(float)) {
    size_t pos = buf_.encode_int32(0, sizeof(float));
    buf_.encode_float(pos, value);
  }

  explicit SimpleInputValue(double value)
    : InputValue(CASS_VALUE_TYPE_DOUBLE)
    , buf_(sizeof(int32_t) + sizeof(double)) {
    size_t pos = buf_.encode_int32(0, sizeof(double));
    buf_.encode_double(pos, value);
  }

  explicit SimpleInputValue(bool value)
    : InputValue(CASS_VALUE_TYPE_BOOLEAN)
    , buf_(sizeof(int32_t) + 1) {
    size_t pos = buf_.encode_int32(0, 1);
    buf_.encode_bool(pos, value);
  }

  explicit SimpleInputValue(CassString value)
    : InputValue(CASS_VALUE_TYPE_TEXT)
    , buf_(sizeof(int32_t) + value.length) {
    size_t pos = buf_.encode_int32(0, value.length);
    buf_.copy(pos, value.data, value.length);
  }

  explicit SimpleInputValue(CassBytes value)
    : InputValue(CASS_VALUE_TYPE_BLOB)
    , buf_(sizeof(int32_t) + value.size) {
    size_t pos = buf_.encode_int32(0, value.size);
    buf_.copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  }

  explicit SimpleInputValue(CassUuid value)
    : InputValue(CASS_VALUE_TYPE_UUID)
    , buf_(sizeof(int32_t) + sizeof(CassUuid)) {
    size_t pos = buf_.encode_int32(0, sizeof(CassUuid));
    buf_.encode_uuid(pos, value);
  }

  explicit SimpleInputValue(CassInet value)
    : InputValue(CASS_VALUE_TYPE_INET)
    , buf_(sizeof(int32_t) + value.address_length) {
    size_t pos = buf_.encode_int32(0, value.address_length);
    buf_.copy(pos, value.address, value.address_length);
  }

  explicit SimpleInputValue(CassDecimal value)
    : InputValue(CASS_VALUE_TYPE_DECIMAL)
    , buf_(sizeof(int32_t) + sizeof(int32_t) + value.varint_size) {
    size_t pos = buf_.encode_int32(0, sizeof(int32_t) + value.varint_size);
    pos = buf_.encode_int32(pos, value.scale);
    buf_.copy(pos, value.varint, value.varint_size);
  }


  virtual size_t get_size() const {
    return buf_.size();
  }

  virtual Buffer encode() const {
    return buf_;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, buf_.data(), buf_.size());
  }

private:
  Buffer buf_;
};

class ShortInputValue : public InputValue {
public:
  explicit ShortInputValue(int32_t value)
    : InputValue(CASS_VALUE_TYPE_INT)
    , buf_(sizeof(uint16_t) + sizeof(int32_t)) {
    size_t pos = buf_.encode_uint16(0, sizeof(int32_t));
    buf_.encode_int32(pos, value);
  }

  explicit ShortInputValue(int64_t value)
    : InputValue(CASS_VALUE_TYPE_BIGINT)
    , buf_(sizeof(uint16_t) + sizeof(int64_t)) {
    size_t pos = buf_.encode_uint16(0, sizeof(int64_t));
    buf_.encode_int64(pos, value);
  }

  explicit ShortInputValue(float value)
    : InputValue(CASS_VALUE_TYPE_FLOAT)
    , buf_(sizeof(uint16_t) + sizeof(float)) {
    size_t pos = buf_.encode_uint16(0, sizeof(float));
    buf_.encode_float(pos, value);
  }

  explicit ShortInputValue(double value)
    : InputValue(CASS_VALUE_TYPE_DOUBLE)
    , buf_(sizeof(uint16_t) + sizeof(double)) {
    size_t pos = buf_.encode_uint16(0, sizeof(double));
    buf_.encode_double(pos, value);
  }

  explicit ShortInputValue(bool value)
    : InputValue(CASS_VALUE_TYPE_BOOLEAN)
    , buf_(sizeof(uint16_t) + 1) {
    size_t pos = buf_.encode_uint16(0, 1);
    buf_.encode_bool(pos, value);
  }

  explicit ShortInputValue(CassString value)
    : InputValue(CASS_VALUE_TYPE_TEXT)
    , buf_(sizeof(uint16_t) + value.length) {
    size_t pos = buf_.encode_uint16(0, value.length);
    buf_.copy(pos, value.data, value.length);
  }

  explicit ShortInputValue(CassBytes value)
    : InputValue(CASS_VALUE_TYPE_BLOB)
    , buf_(sizeof(uint16_t) + value.size) {
    size_t pos = buf_.encode_uint16(0, value.size);
    buf_.copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  }

  explicit ShortInputValue(CassUuid value)
    : InputValue(CASS_VALUE_TYPE_UUID)
    , buf_(sizeof(uint16_t) + sizeof(CassUuid)) {
    size_t pos = buf_.encode_uint16(0, sizeof(CassUuid));
    buf_.encode_uuid(pos, value);
  }

  explicit ShortInputValue(CassInet value)
    : InputValue(CASS_VALUE_TYPE_INET)
    , buf_(sizeof(uint16_t) + value.address_length) {
    size_t pos = buf_.encode_uint16(0, value.address_length);
    buf_.copy(pos, value.address, value.address_length);
  }

  explicit ShortInputValue(CassDecimal value)
    : InputValue(CASS_VALUE_TYPE_DECIMAL)
    , buf_(sizeof(uint16_t) + sizeof(int32_t) + value.varint_size) {
    size_t pos = buf_.encode_uint16(0, sizeof(int32_t) + value.varint_size);
    pos = buf_.encode_int32(pos, value.scale);
    buf_.copy(pos, value.varint, value.varint_size);
  }

  virtual size_t get_size() const {
    return buf_.size();
  }

  virtual Buffer encode() const {
    return buf_;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, buf_.data(), buf_.size());
  }

private:
  Buffer buf_;
};

class CollectionInputValue : public InputValue {
public:
  CollectionInputValue(int protocol_version,
                       CassCollectionType type,
                       size_t item_count)
    : InputValue(type)
    , protocol_version_(protocol_version) {
    items_.reserve(item_count);
  }

  const InputValueVec& items() const { return items_; }

#define APPEND_TYPE(DeclType)     \
  void append(const DeclType value) { \
    if (protocol_version_ < 3) { \
      items_.push_back(SharedRefPtr<const InputValue>(new ShortInputValue(value))); \
    } else { \
      items_.push_back(SharedRefPtr<const InputValue>(new SimpleInputValue(value))); \
    } \
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

  size_t get_count_size() const {
    return (protocol_version_ < 3) ? sizeof(uint16_t) : sizeof(int32_t);
  }

  size_t get_values_size() const {
    size_t size = 0;
    for (InputValueVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      size += (*i)->get_size();
    }
    return size;
  }

  void encode_values(size_t pos, Buffer* buf) const {
    for (InputValueVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      pos = (*i)->copy_encoded(pos, buf);
    }
  }

  virtual size_t get_size() const {
    return sizeof(int32_t) + get_count_size() + get_values_size();
  }

  virtual Buffer encode() const {
    size_t value_size = get_count_size() + get_values_size();

    Buffer buf(sizeof(int32_t) + value_size);

    size_t pos = buf.encode_int32(0, value_size);

    int32_t count
        = ((type_ == CASS_COLLECTION_TYPE_MAP) ? items_.size() / 2 : items_.size());

    if (protocol_version_ < 3) {
      pos = buf.encode_uint16(pos, count);
    } else {
      pos = buf.encode_int32(pos, count);
    }

    encode_values(pos, &buf);

    return buf;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    Buffer encoded = encode();
    return buf->copy(offset, encoded.base, encoded.len);
  }

  int protocol_version_;
  InputValueVec items_;
};

class UserTypeInputValue : public InputValue {
public:
  UserTypeInputValue(const SharedRefPtr<UserType>& user_type)
    : InputValue(CASS_VALUE_TYPE_UDT)
    , user_type_(user_type) {
    items_.resize(user_type->fields().size());
  }

  virtual size_t get_size() const {
    return 0;
  }

  virtual Buffer encode() const {
    return Buffer();
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return 0;
  }

private:
  SharedRefPtr<UserType> user_type_;
  InputValueVec items_;
};


} // namespace cass

#endif


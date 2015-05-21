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
  enum Type {
    NUL,
    CUSTOM,
    SIMPLE,
    SHORT,
    COLLECTION,
    USER_TYPE
  };

  InputValue(int type)
    : type_(type) { }

  int type() const { return type_; }

  virtual ~InputValue() { }
  virtual size_t get_size() const = 0;
  virtual Buffer encode() const = 0;
  virtual size_t copy_encoded(size_t offset, Buffer* buf) const = 0;

private:
  int type_;
};

typedef std::vector<SharedRefPtr<const InputValue> > InputValueVec;

class NullInputValue : public InputValue, public Buffer {
public:
  NullInputValue()
    : InputValue(NUL)
    , Buffer(sizeof(int32_t)) {
    encode_int32(0, -1); // [bytes] "null"
  }

  virtual size_t get_size() const {
    return len;
  }

  virtual Buffer encode() const {
    return *this;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, base, len);
  }
};

class CustomInputValue : public InputValue, public Buffer {
public:
  CustomInputValue(size_t len)
    : InputValue(CUSTOM)
    , Buffer(sizeof(int32_t) + len) {
    encode_int32(0, len);
  }

  virtual size_t get_size() const {
    return len;
  }

  virtual Buffer encode() const {
    return *this;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, base, len);
  }

  char* data() {
    return base + sizeof(int32_t);
  }
};

class SimpleInputValue : public InputValue, public Buffer {
public:
  explicit SimpleInputValue(int32_t value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(int32_t)) {
    size_t pos = encode_int32(0, sizeof(int32_t));
    encode_int32(pos, value);
  }

  explicit SimpleInputValue(int64_t value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(int64_t)) {
    size_t pos = encode_int32(0, sizeof(int64_t));
    encode_int64(pos, value);
  }

  explicit SimpleInputValue(float value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(float)) {
    size_t pos = encode_int32(0, sizeof(float));
    encode_float(pos, value);
  }

  explicit SimpleInputValue(double value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(double)) {
    size_t pos = encode_int32(0, sizeof(double));
    encode_double(pos, value);
  }

  explicit SimpleInputValue(bool value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + 1) {
    size_t pos = encode_int32(0, 1);
    encode_bool(pos, value);
  }

  explicit SimpleInputValue(CassString value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + value.length) {
    size_t pos = encode_int32(0, value.length);
    copy(pos, value.data, value.length);
  }

  explicit SimpleInputValue(CassBytes value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + value.size) {
    size_t pos = encode_int32(0, value.size);
    copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  }

  explicit SimpleInputValue(CassUuid value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(CassUuid)) {
    size_t pos = encode_int32(0, sizeof(CassUuid));
    encode_uuid(pos, value);
  }

  explicit SimpleInputValue(CassInet value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + value.address_length) {
    size_t pos = encode_int32(0, value.address_length);
    copy(pos, value.address, value.address_length);
  }

  explicit SimpleInputValue(CassDecimal value)
    : InputValue(SIMPLE)
    , Buffer(sizeof(int32_t) + sizeof(int32_t) + value.varint_size) {
    size_t pos = encode_int32(0, sizeof(int32_t) + value.varint_size);
    pos = encode_int32(pos, value.scale);
    copy(pos, value.varint, value.varint_size);
  }


  virtual size_t get_size() const {
    return len;
  }

  virtual Buffer encode() const {
    return *this;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, base, len);
  }
};

class ShortInputValue : public InputValue, public Buffer {
public:
  explicit ShortInputValue(int32_t value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(int32_t)) {
    size_t pos = encode_uint16(0, sizeof(int32_t));
    encode_int32(pos, value);
  }

  explicit ShortInputValue(int64_t value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(int64_t)) {
    size_t pos = encode_uint16(0, sizeof(int64_t));
    encode_int64(pos, value);
  }

  explicit ShortInputValue(float value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(float)) {
    size_t pos = encode_uint16(0, sizeof(float));
    encode_float(pos, value);
  }

  explicit ShortInputValue(double value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(double)) {
    size_t pos = encode_uint16(0, sizeof(double));
    encode_double(pos, value);
  }

  explicit ShortInputValue(bool value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + 1) {
    size_t pos = encode_uint16(0, 1);
    encode_bool(pos, value);
  }

  explicit ShortInputValue(CassString value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + value.length) {
    size_t pos = encode_uint16(0, value.length);
    copy(pos, value.data, value.length);
  }

  explicit ShortInputValue(CassBytes value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + value.size) {
    size_t pos = encode_uint16(0, value.size);
    copy(pos, reinterpret_cast<const char*>(value.data), value.size);
  }

  explicit ShortInputValue(CassUuid value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(CassUuid)) {
    size_t pos = encode_uint16(0, sizeof(CassUuid));
    encode_uuid(pos, value);
  }

  explicit ShortInputValue(CassInet value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + value.address_length) {
    size_t pos = encode_uint16(0, value.address_length);
    copy(pos, value.address, value.address_length);
  }

  explicit ShortInputValue(CassDecimal value)
    : InputValue(SHORT)
    , Buffer(sizeof(uint16_t) + sizeof(int32_t) + value.varint_size) {
    size_t pos = encode_uint16(0, sizeof(int32_t) + value.varint_size);
    pos = encode_int32(pos, value.scale);
    copy(pos, value.varint, value.varint_size);
  }

  virtual size_t get_size() const {
    return len;
  }

  virtual Buffer encode() const {
    return *this;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    return buf->copy(offset, base, len);
  }
};

class CollectionInputValue : public InputValue {
public:
  CollectionInputValue(int protocol_version,
                       CassCollectionType type,
                       size_t item_count)
    : InputValue(COLLECTION)
    , protocol_version_(protocol_version)
    , collection_type_(type) {
    items_.reserve(item_count);
  }

  CassCollectionType collection_type() const { return collection_type_; }
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

  size_t get_value_size() const {
    size_t size = ((protocol_version_ < 3) ? sizeof(uint16_t) : sizeof(int32_t));
    for (InputValueVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      size += (*i)->get_size();
    }
    return size;
  }

  virtual size_t get_size() const {
    return sizeof(int32_t) + get_value_size();
  }

  virtual Buffer encode() const {
    size_t value_size = get_value_size();

    Buffer buf(sizeof(int32_t) + value_size);

    size_t pos = buf.encode_int32(0, value_size);

    int32_t count
        = ((collection_type_ == CASS_COLLECTION_TYPE_MAP) ? items_.size() / 2 : items_.size());

    if (protocol_version_ < 3) {
      pos = buf.encode_uint16(pos, count);
    } else {
      pos = buf.encode_int32(pos, count);
    }

    for (InputValueVec::const_iterator i = items_.begin(),
         end = items_.end(); i != end; ++i) {
      pos = (*i)->copy_encoded(pos, &buf);
    }

    return buf;
  }

  virtual size_t copy_encoded(size_t offset, Buffer* buf) const {
    Buffer encoded = encode();
    return buf->copy(offset, encoded.base, encoded.len);
  }

  int protocol_version_;
  CassCollectionType collection_type_;
  InputValueVec items_;
};

class UserTypeInputValue : public InputValue {
public:
};


} // namespace cass

#endif


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

#ifndef __CASS_BUFFER_HPP_INCLUDED__
#define __CASS_BUFFER_HPP_INCLUDED__

#include "ref_counted.hpp"
#include "serialization.hpp"

#include <uv.h>

#include <vector>

namespace cass {

class Buffer : public uv_buf_t {
public:
  Buffer(size_t len) {
    RefBuffer* temp = RefBuffer::create(len);
    this->base = temp->data();
    this->len = len;
    temp->inc_ref();
  }

  Buffer() {
    base = NULL;
    len = 0;
  }

  Buffer(const Buffer& buf) {
    this->base = NULL;
    this->len = 0;
    internal_copy(buf);
  }

  Buffer& operator=(const Buffer& buf) {
    internal_copy(buf);
    return *this;
  }

  ~Buffer() {
    if (base != NULL) {
      ref()->dec_ref();
    }
  }

  char* data() const { return base; }
  size_t size() const { return len; }

  size_t encode_byte(size_t offset, uint8_t value) {
    assert(offset + sizeof(uint8_t) <= len);
    cass::encode_byte(base + offset, value);
    return offset + sizeof(uint8_t);
  }

  size_t encode_uint16(size_t offset, uint16_t value) {
    assert(offset + sizeof(int16_t) <= len);
    cass::encode_uint16(base + offset, value);
    return offset + sizeof(int16_t);
  }

  size_t encode_int32(size_t offset, int32_t value) {
    assert(offset + sizeof(int32_t) <= len);
    cass::encode_int32(base + offset, value);
    return offset + sizeof(int32_t);
  }

  size_t encode_int64(size_t offset, int64_t value) {
    assert(offset + sizeof(int64_t) <= len);
    cass::encode_int64(base + offset, value);
    return offset + sizeof(int64_t);
  }

  size_t encode_float(size_t offset, float value) {
    assert(offset + sizeof(float) <= len);
    cass::encode_float(base + offset, value);
    return offset + sizeof(float);
  }

  size_t encode_double(size_t offset, double value) {
    assert(offset + sizeof(double) <= len);
    cass::encode_double(base + offset, value);
    return offset + sizeof(double);
  }

  size_t encode_bool(size_t offset, bool value) {
    return encode_byte(offset, value);
  }

  size_t encode_long_string(size_t offset, const char* value, int32_t size) {
    size_t pos = encode_int32(offset, size);
    return copy(pos, value, size);
  }

  size_t encode_bytes(size_t offset, const char* value, int32_t size) {
    size_t pos = encode_int32(offset, size);
    if (size > 0) {
      return copy(pos, value, size);
    }
    return pos;
  }

  size_t encode_string(size_t offset, const char* value, uint16_t size) {
    size_t pos = encode_uint16(offset, size);
    return copy(pos, value, size);
  }

  size_t encode_string_list(size_t offset, const std::vector<std::string>& value) {
    size_t pos = encode_uint16(offset, value.size());
    for (std::vector<std::string>::const_iterator it = value.begin(),
         end = value.end(); it != end; ++it) {
      pos = encode_string(pos, it->data(), it->size());
    }
    return pos;
  }

  size_t encode_string_map(size_t offset, const std::map<std::string, std::string>& value) {
    size_t pos = encode_uint16(offset, value.size());
    for (std::map<std::string, std::string>::const_iterator it = value.begin();
         it != value.end(); ++it) {
      pos = encode_string(pos, it->first.c_str(), it->first.size());
      pos = encode_string(pos, it->second.c_str(), it->second.size());
    }
    return pos;
  }

  size_t encode_uuid(size_t offset, CassUuid value) {
    assert(offset + sizeof(CassUuid) <= len);
    cass::encode_uuid(base + offset, value);
    return offset + sizeof(CassUuid);
  }

  size_t copy(size_t offset, const char* value, size_t size) {
    assert(offset + size <= len);
    memcpy(base + offset, value, size);
    return offset + size;
  }

  size_t copy(size_t offset, const uint8_t* source, size_t size) {
    return copy(offset, reinterpret_cast<const char*>(source), size);
  }

  SharedRefPtr<RefBuffer> buffer() const {
    if (base == NULL) {
      return SharedRefPtr<RefBuffer>();
    }
    return SharedRefPtr<RefBuffer>(ref());
  }

  // Testing only!
  RefBuffer* ref() const {
    return reinterpret_cast<RefBuffer*>(base - sizeof(RefBuffer));
  }

private:
  void internal_copy(const Buffer& buf) {
    if (base == buf.base) return;

    RefBuffer* temp = base != NULL ? ref() : NULL;
    if (buf.base != NULL) {
      buf.ref()->inc_ref();
    }
    base = buf.base;
    len = buf.len;
    if (temp != NULL) {
      temp->dec_ref();
    }
  }
};

typedef std::vector<Buffer> BufferVec;

} // namespace cass

#endif

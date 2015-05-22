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

#ifndef __CASS_STATEMENT_HPP_INCLUDED__
#define __CASS_STATEMENT_HPP_INCLUDED__

#include "buffer.hpp"
#include "input_value.hpp"
#include "macros.hpp"
#include "request.hpp"

#include <vector>
#include <string>

#define CASS_VALUE_CHECK_INDEX(i)              \
  if (index >= values_count()) {               \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS; \
  }

namespace cass {

class Statement : public RoutableRequest {
public:
  Statement(uint8_t opcode, uint8_t kind, size_t value_count = 0)
      : RoutableRequest(opcode)
      , values_(value_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind) {}

  Statement(uint8_t opcode, uint8_t kind, size_t value_count,
            const std::vector<size_t>& key_indices,
            const std::string& keyspace)
      : RoutableRequest(opcode, keyspace)
      , values_(value_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind)
      , key_indices_(key_indices) {}

  virtual ~Statement() {}

  void set_skip_metadata(bool skip_metadata) {
    skip_metadata_ = skip_metadata;
  }

  bool skip_metadata() const {
    return skip_metadata_;
  }

  int32_t page_size() const { return page_size_; }

  void set_page_size(int32_t page_size) { page_size_ = page_size; }

  const std::string paging_state() const { return paging_state_; }

  void set_paging_state(const std::string& paging_state) {
    paging_state_ = paging_state;
  }

  uint8_t kind() const { return kind_; }

  virtual const std::string& query() const = 0;

  size_t values_count() const { return values_.size(); }

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(std::string* routing_key)  const;

#define BIND_TYPE(DeclType) \
  CassError bind(size_t index, const DeclType value) { \
    CASS_VALUE_CHECK_INDEX(index); \
    values_[index] \
      = SharedRefPtr<const InputValue>(new SimpleInputValue(value)); \
    return CASS_OK; \
  }
  BIND_TYPE(cass_int32_t)
  BIND_TYPE(cass_int64_t)
  BIND_TYPE(cass_float_t)
  BIND_TYPE(cass_double_t)
  BIND_TYPE(bool)
  BIND_TYPE(CassString)
  BIND_TYPE(CassBytes)
  BIND_TYPE(CassUuid)
  BIND_TYPE(CassInet)
  BIND_TYPE(CassDecimal)
#undef BIND_TYPE

  CassError bind(size_t index, CassNull) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = SharedRefPtr<const InputValue>(new NullInputValue());
    return CASS_OK;
  }

  CassError bind(size_t index, CassCustom custom) {
    CASS_VALUE_CHECK_INDEX(index);
    SharedRefPtr<CustomInputValue> output(new CustomInputValue(custom.output_size));
    *(custom.output) = reinterpret_cast<uint8_t*>(output->data());
    values_[index] = static_cast<SharedRefPtr<const InputValue> >(output);
    return CASS_OK;
  }

  CassError bind(size_t index, const CollectionInputValue* collection) {
    CASS_VALUE_CHECK_INDEX(index);
    if (collection->type() == CASS_COLLECTION_TYPE_MAP &&
        collection->items().size() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values_[index] = SharedRefPtr<const InputValue>(collection);
    return CASS_OK;
  }

  CassError bind(size_t index, const UserTypeInputValue* user_type) {
    CASS_VALUE_CHECK_INDEX(index);
    values_[index] = SharedRefPtr<const InputValue>(user_type);
    return CASS_OK;
  }

  int32_t encode_values(BufferVec*  bufs) const;

private:
  typedef BufferVec ValueVec;

  InputValueVec values_;

  bool skip_metadata_;
  int32_t page_size_;
  std::string paging_state_;
  uint8_t kind_;
  std::vector<size_t> key_indices_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

} // namespace cass

#endif

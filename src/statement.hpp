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
#include "collection.hpp"
#include "constants.hpp"
#include "encode.hpp"
#include "macros.hpp"
#include "request.hpp"
#include "result_metadata.hpp"
#include "result_response.hpp"
#include "types.hpp"
#include "user_type_value.hpp"

#include <vector>
#include <string>

namespace cass {

template<class T>
CassError validate_type(const SharedRefPtr<ResultMetadata>& metadata,
                        size_t index,
                        const T value) {
  IsValidDataType<T> is_valid_type;
  if (index >= metadata->column_count()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  if(!is_valid_type(value, metadata->get_column_definition(index).data_type)) {
      return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
  }
  return CASS_OK;
}

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

  const SharedRefPtr<ResultMetadata>& metadata() const {
    return metadata_;
  }

  size_t values_count() const { return values_.size(); }

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(std::string* routing_key) const;


#define CHECK_INDEX_AND_TYPE(index, value) do { \
  if (index >= values_count()) {                \
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;  \
  }                                             \
  CassError rc = validate_type(index, value);   \
  if (rc != CASS_OK) return rc;                 \
} while(0)

#define BIND_TYPE(DeclType)                            \
  CassError bind(size_t index, const DeclType value) { \
    CHECK_INDEX_AND_TYPE(index, value);                \
    values_[index]  = encode_with_length(value);       \
    return CASS_OK;                                    \
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
  BIND_TYPE(CassNull)

#undef BIND_TYPE

  CassError bind(size_t index, CassCustom custom) {
    CHECK_INDEX_AND_TYPE(index, custom);
    Buffer value(custom.output_size);
    values_[index] = value;
    *(custom.output) = reinterpret_cast<uint8_t*>(value.data());
    return CASS_OK;
  }

  CassError bind(size_t index, const Collection* collection) {
    CHECK_INDEX_AND_TYPE(index, collection);
    if (collection->type() == CASS_COLLECTION_TYPE_MAP &&
        collection->items().size() % 2 != 0) {
      return CASS_ERROR_LIB_INVALID_ITEM_COUNT;
    }
    values_[index] = collection->encode_with_length();
    return CASS_OK;
  }

  CassError bind(size_t index, const UserTypeValue* user_type) {
    CHECK_INDEX_AND_TYPE(index, user_type);
    return CASS_OK;
  }

#undef CHECK_INDEX_AND_TYPE

  int32_t encode_values(BufferVec*  bufs) const;

protected:
  SharedRefPtr<ResultMetadata> metadata_;

private:
  template <class T>
  CassError validate_type(size_t index, const T value) const {
    // This can only validate statements with prepared data
    if (!metadata_) {
      return CASS_OK;
    }
    return cass::validate_type(metadata_, index, value);
    return CASS_OK;
  }

  typedef BufferVec ValueVec;

  ValueVec values_;

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

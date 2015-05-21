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

#ifndef __CASS_OUTPUT_VALUE_HPP_INCLUDED__
#define __CASS_OUTPUT_VALUE_HPP_INCLUDED__

#include "cassandra.h"
#include "result_metadata.hpp"
#include "string_ref.hpp"

namespace cass {

class OutputValue {
public:
  OutputValue()
      : type_(CASS_VALUE_TYPE_UNKNOWN)
      , primary_type_(CASS_VALUE_TYPE_UNKNOWN)
      , secondary_type_(CASS_VALUE_TYPE_UNKNOWN)
      , count_(0) {}

  OutputValue(CassValueType type, char* data, size_t size)
      : type_(type)
      , primary_type_(CASS_VALUE_TYPE_UNKNOWN)
      , secondary_type_(CASS_VALUE_TYPE_UNKNOWN)
      , count_(0)
      , data_(data)
      , size_(size) {}

  OutputValue(CassValueType type, CassValueType primary_type, CassValueType secondary_type,
        int32_t count, char* data, size_t size)
      : type_(type)
      , primary_type_(primary_type)
      , secondary_type_(secondary_type)
      , count_(count)
      , data_(data)
      , size_(size) {}

  OutputValue(const ColumnDefinition* def, int32_t count, char* data, size_t size)
    : type_(static_cast<CassValueType>(def->type))
    , primary_type_(static_cast<CassValueType>(def->collection_primary_type))
    , secondary_type_(static_cast<CassValueType>(def->collection_secondary_type))
    , count_(count)
    , data_(data)
    , size_(size) {}

  CassValueType type() const { return type_; }

  CassValueType primary_type() const {
    return primary_type_;
  }

  CassValueType secondary_type() const {
    return secondary_type_;
  }

  bool is_null() const {
    return size_ < 0;
  }

  bool is_collection() const { return is_collection(type_); }
  static bool is_collection(CassValueType t);

  int32_t count() const {
    return count_;
  }

  char* data() const { return data_; }
  int32_t size() const { return size_; }

  StringRef to_string_ref() const {
    if (size_ < 0) return StringRef();
    return StringRef(data_, size_);
  }

  std::string to_string() const {
    return to_string_ref().to_string();
  }

private:
  CassValueType type_;
  CassValueType primary_type_;
  CassValueType secondary_type_;
  int32_t count_;

  char* data_;
  int32_t size_;
};

typedef std::vector<OutputValue> OutputValueVec;

} // namespace cass

#endif

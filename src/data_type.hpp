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

#ifndef __CASS_DATA_TYPE_HPP_INCLUDED__
#define __CASS_DATA_TYPE_HPP_INCLUDED__

#include "cassandra.h"
#include "hash_index.hpp"
#include "ref_counted.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class DataType : public RefCounted<DataType> {
public:
  DataType(CassValueType type)
    : type_(type) { }

  virtual ~DataType() { }

  CassValueType type() const { return type_; }

  virtual bool is_frozen() const { return false; }

private:
  CassValueType type_;
};

typedef std::vector<SharedRefPtr<DataType> > DataTypeVec;

class CollectionType : public DataType {
public:
  CollectionType(CassValueType type, const DataTypeVec& types, bool frozen)
    : DataType(type)
    , types_(types)
    , frozen_(frozen) { }

  virtual bool is_frozen() const { return frozen_; }
  const DataTypeVec& types() const { return types_; }

public:
  static SharedRefPtr<DataType> list(SharedRefPtr<DataType> element_type, bool frozen) {
    DataTypeVec types;
    types.push_back(element_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_LIST, types, frozen));
  }

  static SharedRefPtr<DataType> set(SharedRefPtr<DataType> element_type, bool frozen) {
    DataTypeVec types;
    types.push_back(element_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_SET, types, frozen));
  }

  static SharedRefPtr<DataType> map(SharedRefPtr<DataType> key_type, SharedRefPtr<DataType> value_type, bool frozen) {
    DataTypeVec types;
    types.push_back(key_type);
    types.push_back(value_type);
    return SharedRefPtr<DataType>(new CollectionType(CASS_VALUE_TYPE_MAP, types, frozen));
  }

private:
  DataTypeVec types_;
  bool frozen_;
};

class CustomType : public DataType {
public:
  CustomType(const std::string& class_name)
    : DataType(CASS_VALUE_TYPE_CUSTOM)
    , class_name_(class_name) { }

  const std::string& class_name() const { return class_name_; }
private:
  std::string class_name_;
};

class UserType : public DataType {
public:
  struct Field : public HashIndex::Entry {
    Field(const std::string& field_name,
          SharedRefPtr<DataType> type)
      : field_name(field_name)
      , type(type) {
      name = field_name.data();
      name_size = field_name.size();
    }

    std::string field_name;
    SharedRefPtr<DataType> type;
  };

  typedef std::vector<Field> FieldVec;

  UserType(const std::string& keyspace,
           const std::string& type_name,
           const FieldVec& fields)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , fields_(fields)
    , index_(fields.size()) {
    for (size_t i = 0; i < fields_.size(); ++i) {
      Field* field = &fields_[i];
      field->index = i;
      index_.insert(field);
    }
  }

  const std::string& keyspace() const { return keyspace_; }
  const std::string& type_name() const { return type_name_; }
  const FieldVec& fields() const { return fields_; }

  size_t get_indexes(StringRef name, HashIndex::IndexVec* result) const {
    return index_.get(name, result);
  }

private:
  std::string keyspace_;
  std::string type_name_;
  FieldVec fields_;
  HashIndex index_;
};

class TupleType : public DataType {
public:
  TupleType(const DataTypeVec& types)
    : DataType(CASS_VALUE_TYPE_TUPLE)
    , types_(types) { }

  const DataTypeVec& types() const { return types_; }

private:
  DataTypeVec types_;
};

} // namespace cass

#endif

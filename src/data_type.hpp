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
#include "types.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class Collection;
class UserTypeValue;

class DataType : public RefCounted<DataType> {
public:
  DataType(CassValueType value_type)
    : value_type_(value_type) { }

  virtual ~DataType() { }

  CassValueType value_type() const { return value_type_; }

  bool is_collection() const  {
    return value_type_ == CASS_VALUE_TYPE_LIST ||
        value_type_ == CASS_VALUE_TYPE_MAP ||
        value_type_ == CASS_VALUE_TYPE_SET;
  }

  bool is_map() const  {
    return value_type_ == CASS_VALUE_TYPE_MAP;
  }

  virtual bool is_frozen() const { return false; }
  virtual bool equals(const SharedRefPtr<DataType>& data_type) const {
    return value_type_ == data_type->value_type_;
  }

private:
  CassValueType value_type_;
};

typedef std::vector<SharedRefPtr<DataType> > DataTypeVec;

class CollectionType : public DataType {
public:
  CollectionType(CassValueType collection_type, const DataTypeVec& types, bool frozen)
    : DataType(collection_type)
    , types_(types)
    , frozen_(frozen) { }

  CollectionType(CassValueType primary_type,
                 CassValueType secondary_type)
    : DataType(CASS_VALUE_TYPE_MAP) {
    types_.push_back(SharedRefPtr<DataType>(new DataType(primary_type)));
    types_.push_back(SharedRefPtr<DataType>(new DataType(secondary_type)));
    frozen_ = false;
  }

  virtual bool is_frozen() const { return frozen_; }
  const DataTypeVec& types() const { return types_; }

  virtual bool equals(const SharedRefPtr<DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_LIST ||
           value_type() == CASS_VALUE_TYPE_SET ||
           value_type() == CASS_VALUE_TYPE_MAP);
    if (value_type() == data_type->value_type()) {
      const SharedRefPtr<CollectionType>& collection_type(data_type);
      assert(types_.size() == collection_type->types_.size());
      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(collection_type->types_[i])) {
          return false;
        }
      }
    }
    return false;
  }

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
      name = StringRef(this->field_name);
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

  size_t get_indices(StringRef name, HashIndex::IndexVec* result) const {
    return index_.get(name, result);
  }

  virtual bool equals(const SharedRefPtr<DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_UDT);
    if (data_type->value_type() == CASS_VALUE_TYPE_UDT) {
      const SharedRefPtr<UserType>& user_type(data_type);
      if (fields_.size() != user_type->fields_.size()) {
        return false;
      }
      for (size_t i = 0; i < fields_.size(); ++i) {
        if (fields_[i].name != user_type->fields_[i].name ||
            !fields_[i].type->equals(user_type->fields_[i].type)) {
          return false;
        }
      }
    }
    return false;
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

  virtual bool equals(const SharedRefPtr<DataType>& data_type) const {
    assert(value_type() == CASS_VALUE_TYPE_TUPLE);
    if (value_type() == data_type->value_type()) {
      const SharedRefPtr<TupleType>& tuple_type(data_type);
      if (types_.size() != tuple_type->types_.size()) {
        return false;
      }
      for (size_t i = 0; i < types_.size(); ++i) {
        if(!types_[i]->equals(tuple_type->types_[i])) {
          return false;
        }
      }
    }
    return false;
  }

private:
  DataTypeVec types_;
};

template<class T>
struct IsValidDataType;

template<>
struct IsValidDataType<CassNull> {
  bool operator()(CassNull, const SharedRefPtr<DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<cass_int32_t> {
  bool operator()(cass_int32_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INT;
  }
};

template<>
struct IsValidDataType<cass_int64_t> {
  bool operator()(cass_int64_t, const SharedRefPtr<DataType>& data_type) const {
    int value_type = data_type->value_type();
    return value_type == CASS_VALUE_TYPE_BIGINT ||
        value_type == CASS_VALUE_TYPE_COUNTER ||
        value_type == CASS_VALUE_TYPE_TIMESTAMP;
  }
};

template<>
struct IsValidDataType<cass_float_t> {
  bool operator()(cass_float_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_FLOAT;
  }
};

template<>
struct IsValidDataType<cass_double_t> {
  bool operator()(cass_double_t, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DOUBLE;
  }
};

template<>
struct IsValidDataType<bool> {
  bool operator()(bool, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_BOOLEAN;
  }
};

template<>
struct IsValidDataType<CassString> {
  bool operator()(CassString, const SharedRefPtr<DataType>& data_type) const {
    int value_type = data_type->value_type();
    return value_type == CASS_VALUE_TYPE_ASCII ||
        value_type == CASS_VALUE_TYPE_TEXT ||
        value_type == CASS_VALUE_TYPE_VARCHAR;
  }
};

template<>
struct IsValidDataType<CassBytes> {
  bool operator()(CassBytes, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_BLOB;
  }
};

template<>
struct IsValidDataType<CassUuid> {
  bool operator()(CassUuid, const SharedRefPtr<DataType>& data_type) const {
    int value_type = data_type->value_type();
    return value_type == CASS_VALUE_TYPE_TIMEUUID ||
        value_type == CASS_VALUE_TYPE_UUID;
  }
};

template<>
struct IsValidDataType<CassInet> {
  bool operator()(CassInet, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_INET;
  }
};

template<>
struct IsValidDataType<CassDecimal> {
  bool operator()(CassDecimal, const SharedRefPtr<DataType>& data_type) const {
    return data_type->value_type() == CASS_VALUE_TYPE_DECIMAL;
  }
};

template<>
struct IsValidDataType<CassCustom> {
  bool operator()(CassCustom, const SharedRefPtr<DataType>& data_type) const {
    return true;
  }
};

template<>
struct IsValidDataType<const Collection*> {
  bool operator()(const Collection* value, const SharedRefPtr<DataType>& data_type) const;
};

template<>
struct IsValidDataType<const UserTypeValue*> {
  bool operator()(const UserTypeValue* value, const SharedRefPtr<DataType>& data_type) const;
};

} // namespace cass

#endif

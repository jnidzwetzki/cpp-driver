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

#ifndef __CASS_TYPE_PARSER_HPP_INCLUDED__
#define __CASS_TYPE_PARSER_HPP_INCLUDED__

#include "cassandra.h"
#include "output_value.hpp"
#include "ref_counted.hpp"

#include <list>
#include <map>
#include <string>
#include <sstream>

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

class ParseResult : public RefCounted<ParseResult> {
public:
  typedef std::vector<bool> ReversedVec;
  typedef std::map<std::string, SharedRefPtr<DataType> > CollectionMap;

  ParseResult(SharedRefPtr<DataType> type, bool reversed)
    : is_composite_(false) {
    types_.push_back(type);
    reversed_.push_back(reversed);
  }

  ParseResult(bool is_composite,
              const DataTypeVec& types,
              ReversedVec reversed,
              CollectionMap collections)
    : is_composite_(is_composite)
    , types_(types)
    , reversed_(reversed)
    , collections_(collections) { }

  bool is_composite() const { return is_composite_; }
  const DataTypeVec& types() const { return types_; }
  const ReversedVec& reversed() const  { return reversed_; }
  const CollectionMap& collections() const  { return collections_; }

private:
  bool is_composite_;
  DataTypeVec types_;
  ReversedVec reversed_;
  CollectionMap collections_;
};

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
  struct Field {
    Field(const std::string& name,
          SharedRefPtr<DataType> type)
      : name(name)
      , type(type) { }
    std::string name;
    SharedRefPtr<DataType> type;
  };

  typedef std::vector<Field> FieldVec;

  UserType(const std::string& keyspace,
           const std::string& type_name,
           const FieldVec& fields)
    : DataType(CASS_VALUE_TYPE_UDT)
    , keyspace_(keyspace)
    , type_name_(type_name)
    , fields_(fields) { }

  const std::string& keyspace() const { return keyspace_; }
  const std::string& type_name() const { return type_name_; }
  const FieldVec& fields() const { return fields_; }

private:
  std::string keyspace_;
  std::string type_name_;
  FieldVec fields_;
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

class TypeParser {
public:
  static bool is_reversed(const std::string& type);
  static bool is_frozen(const std::string& type);
  static bool is_composite(const std::string& type);
  static bool is_collection(const std::string& type);

  static bool is_user_type(const std::string& type);
  static bool is_tuple_type(const std::string& type);

  static SharedRefPtr<DataType> parse_one(const std::string& type);
  static SharedRefPtr<ParseResult> parse_with_composite(const std::string& type);

  class TypeMapper {
  public:
    TypeMapper();
    CassValueType operator[](const std::string& cass_type) const;
  private:
    typedef std::map<std::string, CassValueType> NameTypeMap;
    NameTypeMap name_type_map_;
  };

private:
  static bool get_nested_class_name(const std::string& type, std::string* class_name);

  typedef std::vector<std::string> TypeParamsVec;
  typedef std::vector<std::pair<std::string, std::string> > NameAndTypeParamsVec;

  class Parser {
  public:
    Parser(const std::string& str, size_t index)
      : str_(str)
      , index_(index) {}

    void skip() { ++index_; }
    void skip_blank();
    bool skip_blank_and_comma();

    bool read_one(std::string* name_and_args);
    void get_next_name(std::string* name = NULL);
    bool get_type_params(TypeParamsVec* params);
    bool get_name_and_type_params(NameAndTypeParamsVec* params);
    bool get_collection_params(NameAndTypeParamsVec* params);

  private:
    bool read_raw_arguments(std::string* args);
    void read_next_identifier(std::string* name);

    static void parse_error(const std::string& str,
                            size_t index,
                            const char* error);

    bool is_eos() const {
      return index_ >= str_.length();
    }

    static bool is_identifier_char(int c) {
      return (c >= '0' && c <= '9')
          || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
          || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
    }

    static bool is_blank(int c) {
      return c == ' ' || c == '\t' || c == '\n';
    }

  private:
    const std::string str_;
    size_t index_;
  };

  TypeParser();

  const static TypeMapper type_map_;
};

} // namespace cass

#endif

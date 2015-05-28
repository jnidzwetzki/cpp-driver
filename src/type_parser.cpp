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

#include "type_parser.hpp"

#include "common.hpp"
#include "logger.hpp"
#include "scoped_ptr.hpp"

#include <sstream>

#define REVERSED_TYPE "org.apache.cassandra.db.marshal.ReversedType"
#define FROZEN_TYPE "org.apache.cassandra.db.marshal.FrozenType"
#define COMPOSITE_TYPE "org.apache.cassandra.db.marshal.CompositeType"
#define COLLECTION_TYPE "org.apache.cassandra.db.marshal.ColumnToCollectionType"

#define LIST_TYPE "org.apache.cassandra.db.marshal.ListType"
#define SET_TYPE "org.apache.cassandra.db.marshal.SetType"
#define MAP_TYPE "org.apache.cassandra.db.marshal.MapType"
#define UDT_TYPE "org.apache.cassandra.db.marshal.UserType"
#define TUPLE_TYPE "org.apache.cassandra.db.marshal.TupleType"

namespace cass {

int hex_value(int c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  } else if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  return -1;
}

bool from_hex(const std::string& hex, std::string* result) {
  if ((hex.length() & 1) == 1) { // Invalid if not divisible by 2
    return false;
  }
  size_t size = hex.length() / 2;
  result->resize(size);
  for (size_t i = 0; i < size; ++i) {
    int half0 = hex_value(hex[i * 2]);
    int half1 = hex_value(hex[i * 2 + 1]);
    if (half0 < 0 || half1 < 0) {
      return false;
    }
    (*result)[i] = static_cast<char>((static_cast<uint8_t>(half0) << 4) | static_cast<uint8_t>(half1));
  }
  return true;
}

TypeParser::TypeMapper::TypeMapper() {
  name_type_map_["org.apache.cassandra.db.marshal.AsciiType"] = CASS_VALUE_TYPE_ASCII;
  name_type_map_["org.apache.cassandra.db.marshal.LongType"] = CASS_VALUE_TYPE_BIGINT;
  name_type_map_["org.apache.cassandra.db.marshal.BytesType"] = CASS_VALUE_TYPE_BLOB;
  name_type_map_["org.apache.cassandra.db.marshal.BooleanType"] = CASS_VALUE_TYPE_BOOLEAN;
  name_type_map_["org.apache.cassandra.db.marshal.CounterColumnType"] = CASS_VALUE_TYPE_COUNTER;
  name_type_map_["org.apache.cassandra.db.marshal.DecimalType"] = CASS_VALUE_TYPE_DECIMAL;
  name_type_map_["org.apache.cassandra.db.marshal.DoubleType"] = CASS_VALUE_TYPE_DOUBLE;
  name_type_map_["org.apache.cassandra.db.marshal.FloatType"] = CASS_VALUE_TYPE_FLOAT;
  name_type_map_["org.apache.cassandra.db.marshal.InetAddressType"] = CASS_VALUE_TYPE_INET;
  name_type_map_["org.apache.cassandra.db.marshal.Int32Type"] = CASS_VALUE_TYPE_INT;
  name_type_map_["org.apache.cassandra.db.marshal.UTF8Type"] = CASS_VALUE_TYPE_TEXT;
  name_type_map_["org.apache.cassandra.db.marshal.TimestampType"] = CASS_VALUE_TYPE_TIMESTAMP;
  name_type_map_["org.apache.cassandra.db.marshal.DateType"] = CASS_VALUE_TYPE_TIMESTAMP;
  name_type_map_["org.apache.cassandra.db.marshal.UUIDType"] = CASS_VALUE_TYPE_UUID;
  name_type_map_["org.apache.cassandra.db.marshal.IntegerType"] = CASS_VALUE_TYPE_INT;
  name_type_map_["org.apache.cassandra.db.marshal.TimeUUIDType"] = CASS_VALUE_TYPE_TIMEUUID;
}

CassValueType TypeParser::TypeMapper::operator[](const std::string& type_name) const {
  NameTypeMap::const_iterator itr = name_type_map_.find(type_name);
  if (itr != name_type_map_.end()) {
    return itr->second;
  } else {
    return CASS_VALUE_TYPE_UNKNOWN;
  }
}

bool TypeParser::is_reversed(const std::string& type) {
  return starts_with(type, REVERSED_TYPE);
}

bool TypeParser::is_frozen(const std::string& type) {
  return starts_with(type, FROZEN_TYPE);
}

bool TypeParser::is_composite(const std::string& type) {
  return starts_with(type, COMPOSITE_TYPE);
}

bool TypeParser::is_collection(const std::string& type) {
  return starts_with(type, COLLECTION_TYPE);
}

bool TypeParser::is_user_type(const std::string& type) {
  return starts_with(type, UDT_TYPE);
}

bool TypeParser::is_tuple_type(const std::string& type) {
  return starts_with(type, TUPLE_TYPE);
}

SharedRefPtr<DataType> TypeParser::parse_one(const std::string& type) {
  bool frozen = is_frozen(type);

  std::string class_name;

  if (is_reversed(type) || frozen) {
    if (!get_nested_class_name(type, &class_name)) {
      return SharedRefPtr<DataType>();
    }
  } else {
    class_name = type;
  }

  Parser parser(class_name, 0);
  std::string next;

  parser.get_next_name(&next);

  if (starts_with(next, LIST_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.empty()) {
      return SharedRefPtr<DataType>();
    }
    SharedRefPtr<DataType> element_type(parse_one(params[0]));
    if (!element_type) {
      return SharedRefPtr<DataType>();
    }
    return CollectionType::list(element_type);
  } else if(starts_with(next, SET_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.empty()) {
      return SharedRefPtr<DataType>();
    }
    SharedRefPtr<DataType> element_type(parse_one(params[0]));
    if (!element_type) {
      return SharedRefPtr<DataType>();
    }
    return CollectionType::set(element_type);
  } else if(starts_with(next, MAP_TYPE)) {
    TypeParamsVec params;
    if (!parser.get_type_params(&params) || params.size() < 2) {
      return SharedRefPtr<DataType>();
    }
    SharedRefPtr<DataType> key_type(parse_one(params[0]));
    SharedRefPtr<DataType> value_type(parse_one(params[1]));
    if (!key_type || !value_type) {
      return SharedRefPtr<DataType>();
    }
    return CollectionType::map(key_type, value_type);
  }

  if (frozen) {
    LOG_WARN("Got a frozen type for something other than a collection, "
             "this driver might be too old for your version of Cassandra");
  }

  if (is_user_type(next)) {
    parser.skip(); // Skip '('

    std::string keyspace;
    if (!parser.read_one(&keyspace)) {
      return SharedRefPtr<DataType>();
    }
    parser.skip_blank_and_comma();

    std::string hex;
    if (!parser.read_one(&hex)) {
      return SharedRefPtr<DataType>();
    }

    std::string type_name;
    if (!from_hex(hex, &type_name)) {
      LOG_ERROR("Invalid hex string \"%s\" for parameter", hex.c_str());
      return SharedRefPtr<DataType>();
    }

    if (keyspace.empty() || type_name.empty()) {
      LOG_ERROR("UDT has no keyspace or type name");
      return SharedRefPtr<DataType>();
    }

    parser.skip_blank_and_comma();
    NameAndTypeParamsVec raw_fields;
    if (!parser.get_name_and_type_params(&raw_fields)) {
      return SharedRefPtr<DataType>();
    }

    UserType::FieldVec fields;
    for (NameAndTypeParamsVec::const_iterator i = raw_fields.begin(),
         end = raw_fields.end(); i != end; ++i) {
      SharedRefPtr<DataType> data_type = parse_one(i->second);
      if (!data_type) {
        return SharedRefPtr<DataType>();
      }
      fields.push_back(UserType::Field(i->first, data_type));
    }

    return SharedRefPtr<DataType>(new UserType(keyspace, type_name, fields));
  }

  if (is_tuple_type(type)) {
    TypeParamsVec raw_types;
    if (!parser.get_type_params(&raw_types)) {
      return SharedRefPtr<DataType>();
    }

    DataTypeVec types;
    for (TypeParamsVec::const_iterator i = raw_types.begin(),
         end = raw_types.end(); i != end; ++i) {
      SharedRefPtr<DataType> data_type = parse_one(*i);
      if (!data_type) {
        return SharedRefPtr<DataType>();
      }
      types.push_back(data_type);
    }

    return CollectionType::tuple(types);
  }

  CassValueType t = type_map_[next];
  return t == CASS_VALUE_TYPE_UNKNOWN ? SharedRefPtr<DataType>(new CustomType(next))
                                      : SharedRefPtr<DataType>(new DataType(t));
}

SharedRefPtr<ParseResult> TypeParser::parse_with_composite(const std::string& type) {
  Parser parser(type, 0);

  std::string next;
  parser.get_next_name(&next);

  if (!is_composite(next)) {
    SharedRefPtr<DataType> data_type = parse_one(type);
    if (!data_type) {
      return SharedRefPtr<ParseResult>();
    }
    return SharedRefPtr<ParseResult>(new ParseResult(data_type, is_reversed(next)));
  }

  TypeParamsVec sub_class_names;

  if (!parser.get_type_params(&sub_class_names)) {
    return SharedRefPtr<ParseResult>();
  }

  if (sub_class_names.empty()) {
    LOG_ERROR("Expected at least one subclass type for a composite type");
    return SharedRefPtr<ParseResult>();
  }

  ParseResult::CollectionMap collections;
  const std::string& last = sub_class_names.back();
  size_t count = sub_class_names.size();
  if (is_collection(last)) {
    count--;

    Parser collection_parser(last, 0);
    collection_parser.get_next_name();
    NameAndTypeParamsVec params;
    if (!collection_parser.get_collection_params(&params)) {
      return SharedRefPtr<ParseResult>();
    }

    for (NameAndTypeParamsVec::const_iterator i = params.begin(),
         end = params.end(); i != end; ++i) {
      SharedRefPtr<DataType> data_type = parse_one(i->second);
      if (!data_type) {
        return SharedRefPtr<ParseResult>();
      }
      collections[i->first] = data_type;
    }
  }

  DataTypeVec types;
  ParseResult::ReversedVec reversed;
  for (size_t i = 0; i < count; ++i) {
    SharedRefPtr<DataType> data_type = parse_one(sub_class_names[i]);
    if (!data_type) {
      return SharedRefPtr<ParseResult>();
    }
    types.push_back(data_type);
    reversed.push_back(is_reversed(sub_class_names[i]));
  }

  return SharedRefPtr<ParseResult>(new ParseResult(true, types, reversed, collections));
}

bool TypeParser::get_nested_class_name(const std::string& type, std::string* class_name) {
  Parser parser(type, 0);
  parser.get_next_name();
  TypeParamsVec params;
  parser.get_type_params(&params);
  if (params.size() != 1) {
    return false;
  }
  *class_name = params[0];
  return true;
}

const TypeParser::TypeMapper TypeParser::type_map_;

bool TypeParser::Parser::read_one(std::string* name_and_args) {
  std::string name;
  get_next_name(&name);
  std::string args;
  if (!read_raw_arguments(&args)) {
    return false;
  }
  *name_and_args = name + args;
  return true;
}

void TypeParser::Parser::get_next_name(std::string* name) {
  skip_blank();
  read_next_identifier(name);
}

bool TypeParser::Parser::get_type_params(TypeParamsVec* params) {
  if (is_eos()) {
    params->clear();
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_,  "Expected '(' before type parameters");
    return false;
  }

  ++index_; // Skip '('

  while (skip_blank_and_comma()) {
    if (str_[index_] == ')') {
      ++index_;
      return true;
    }

    std::string param;
    if (!read_one(&param)) {
      return false;
    }
    params->push_back(param);
  }

  parse_error(str_, index_,  "Unexpected end of string");
  return false;
}

bool TypeParser::Parser::get_name_and_type_params(NameAndTypeParamsVec* params) {
  while (skip_blank_and_comma()) {
    if (str_[index_] == ')') {
      ++index_;
      return true;
    }

    std::string hex;
    read_next_identifier(&hex);

    std::string name;
    if (!from_hex(hex, &name)) {
      LOG_ERROR("Invalid hex string \"%s\" for parameter", hex.c_str());
      return SharedRefPtr<DataType>();
    }

    skip_blank();

    if (str_[index_] != ':') {
      parse_error(str_, index_,  "Expected ':'");
      return false;
    }

    ++index_;
    skip_blank();

    std::string type;

    if (!read_one(&type)) {
      return false;
    }

    params->push_back(std::make_pair(name, type));
  }

  parse_error(str_, index_,  "Unexpected end of string");
  return false;
}

bool TypeParser::Parser::get_collection_params(NameAndTypeParamsVec* params) {
  if (is_eos()) {
    params->clear();
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_,  "Expected '(' before collection parameters");
    return false;
  }

  ++index_; // Skip '('

  return get_name_and_type_params(params);
}

void TypeParser::Parser::skip_blank() {
  while (!is_eos() && is_blank(str_[index_])) {
    ++index_;
  }
}

bool TypeParser::Parser::skip_blank_and_comma() {
  bool comma_found = false;
  while (!is_eos()) {
    int c = str_[index_];
    if (c == ',') {
      if (comma_found) {
        return true;
      } else {
        comma_found = true;
      }
    } else if (!is_blank(c)) {
      return true;
    }
    ++index_;
  }
  return false;
}

bool TypeParser::Parser::read_raw_arguments(std::string* args) {
  skip_blank();

  if (is_eos() || str_[index_] == ')' || str_[index_] == ',') {
    *args = "";
    return true;
  }

  if (str_[index_] != '(') {
    parse_error(str_, index_, "Expected '('");
    return false;
  }

  int i = index_;
  int open = 1;
  while (open > 0) {
    ++index_;

    if (is_eos()) {
      parse_error(str_, index_, "Expected ')'");
      return false;
    }

    if (str_[index_] == '(') {
      open++;
    } else if (str_[index_] == ')') {
      open--;
    }
  }

  ++index_; // Skip ')'
  *args = str_.substr(i, index_);
  return true;
}

void TypeParser::Parser::read_next_identifier(std::string* name) {
  size_t i = index_;
  while (!is_eos() && is_identifier_char(str_[index_]))
    ++index_;
  if (name != NULL) {
    if (index_ > i) {
      *name = str_.substr(i, index_ - i);
    } else{
      name->clear();
    }
  }
}

void TypeParser::Parser::parse_error(const std::string& str,
                                     size_t index,
                                     const char* error) {
  LOG_ERROR("Error parsing '%s' at %u index: %s",
            str.c_str(),
            static_cast<unsigned int>(index),
            error);
}

} // namespace cass

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

#include "output_value.hpp"

#include "types.hpp"


extern "C" {

CassCollection* cass_collection_new(CassCollectionType type, size_t element_count) {
  cass::CollectionInputValue* collection = new cass::CollectionInputValue(2, type, element_count);
  collection->inc_ref();
  return CassCollection::to(collection);
}

void cass_collection_free(CassCollection* collection) {
  collection->dec_ref();
}

CassError cass_collection_append_int32(CassCollection* collection,
                                       cass_int32_t value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_int64(CassCollection* collection,
                                       cass_int64_t value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_float(CassCollection* collection,
                                       cass_float_t value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_double(CassCollection* collection,
                                        cass_double_t value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_bool(CassCollection* collection,
                                      cass_bool_t value) {
  collection->append(value == cass_true);
  return CASS_OK;
}

CassError cass_collection_append_string(CassCollection* collection,
                                        const char* value) {
  return cass_collection_append_string_n(collection, value, strlen(value));
}

CassError cass_collection_append_string_n(CassCollection* collection,
                                          const char* value,
                                          size_t value_length) {
  cass::CassString s = { value, value_length };
  collection->append(s);
  return CASS_OK;
}

CassError cass_collection_append_bytes(CassCollection* collection,
                                       const cass_byte_t* value,
                                       size_t value_size) {
  cass::CassBytes b = { value, value_size };
  collection->append(b);
  return CASS_OK;
}

CassError cass_collection_append_uuid(CassCollection* collection,
                                      CassUuid value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_inet(CassCollection* collection,
                                      CassInet value) {
  collection->append(value);
  return CASS_OK;
}

CassError cass_collection_append_decimal(CassCollection* collection,
                                         const cass_byte_t* varint,
                                         size_t varint_size,
                                         cass_int32_t scale) {
  cass::CassDecimal d = { varint, varint_size, scale };
  collection->append(d);
  return CASS_OK;
}

} // extern "C"

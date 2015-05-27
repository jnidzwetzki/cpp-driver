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

#include "collection.hpp"

#include "external_types.hpp"
#include "macros.hpp"

#include <string.h>

extern "C" {

CassCollection* cass_collection_new(CassSession* session,
                                    CassCollectionType type,
                                    size_t item_count) {
  return CassCollection::to(new cass::Collection(session->protocol_version(),
                                                 type,
                                                 item_count));
}

void cass_collection_free(CassCollection* collection) {
  delete collection->from();
}

#define CASS_COLLECTION_APPEND(Name, Params, Value) \
 CassError cass_collection_append_##Name(CassCollection* collection Params) { \
   collection->append(Value); \
   return CASS_OK; \
 } \

CASS_COLLECTION_APPEND(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_COLLECTION_APPEND(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_COLLECTION_APPEND(float, ONE_PARAM_(cass_float_t value), value)
CASS_COLLECTION_APPEND(double, ONE_PARAM_(cass_double_t value), value)
CASS_COLLECTION_APPEND(bool, ONE_PARAM_(cass_bool_t value), value == cass_true)
CASS_COLLECTION_APPEND(uuid, ONE_PARAM_(CassUuid value), value)
CASS_COLLECTION_APPEND(inet, ONE_PARAM_(CassInet value), value)
CASS_COLLECTION_APPEND(bytes,
                       TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                       cass::CassBytes(value, value_size))
CASS_COLLECTION_APPEND(decimal,
                       THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                       cass::CassDecimal(varint, varint_size, scale))

#undef CASS_COLLECTION_APPEND

CassError cass_collection_append_string(CassCollection* collection,
                                        const char* value) {
  collection->append(cass::CassString(value, strlen(value)));
  return CASS_OK;
}

CassError cass_collection_append_string_n(CassCollection* collection,
                                          const char* value,
                                          size_t value_length) {
  collection->append(cass::CassString(value, value_length));
  return CASS_OK;
}

} // extern "C"

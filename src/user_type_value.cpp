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

#include "user_type_value.hpp"

#include "macros.hpp"
#include "external_types.hpp"

#include <string.h>

namespace cass {

template<class T>
CassError bind_by_name(cass::UserTypeValue* user_type,
                       StringRef name,
                       T value) {
  cass::HashIndex::IndexVec indices;
  user_type->get_item_indices(name, &indices);

  if (indices.empty()) {
    return CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
  }

  for (cass::HashIndex::IndexVec::const_iterator it = indices.begin(),
       end = indices.end(); it != end; ++it) {
    size_t index = *it;
    CassError rc = user_type->set(index, value);
    if (rc != CASS_OK) {
      return rc;
    }
  }

  return CASS_OK;
}

} // namespace cass

extern "C" {

CassUserType* cass_user_type_new(CassSession* session,
                                 const char* keyspace,
                                 const char* type_name) {
  return cass_user_type_new_n(session,
                              keyspace, strlen(keyspace),
                              type_name, strlen(type_name));
}

CassUserType* cass_user_type_new_n(CassSession* session,
                                   const char* keyspace,
                                   size_t keyspace_length,
                                   const char* type_name,
                                   size_t type_name_length) {
  cass::SharedRefPtr<cass::UserType> user_type
      = session->get_user_type(std::string(keyspace, keyspace_length),
                               std::string(type_name, type_name_length));
  if (!user_type) return NULL;
  return CassUserType::to(new cass::UserTypeValue(user_type));
}


#if 0
#define CASS_USER_TYPE_SET(Name, Params, Value) \
  CassError cass_user_type_set_##Name(CassUserType* user_type,\
                                      size_t index Params) { \
    return user_type->set(index, Value); \
  } \
  CassError cass_user_type_set_##Name##_by_name(CassUserType* user_type, \
                                                const char* name Params) { \
    return cass::bind_by_name(user_type, cass::StringRef(name), Value); \
  } \
  CassError cass_user_type_set_##Name##_by_name_n(CassUserType* user_type, \
                                                  const char* name, \
                                                  size_t name_length Params) { \
    return cass::bind_by_name(user_type, cass::StringRef(name, name_length), Value); \
  }

CASS_USER_TYPE_SET(int32, ONE_PARAM_(cass_int32_t value), value)
CASS_USER_TYPE_SET(int64, ONE_PARAM_(cass_int64_t value), value)
CASS_USER_TYPE_SET(float, ONE_PARAM_(cass_float_t value), value)
CASS_USER_TYPE_SET(double, ONE_PARAM_(cass_double_t value), value)
CASS_USER_TYPE_SET(bool, ONE_PARAM_(cass_bool_t value), value == cass_true)
CASS_USER_TYPE_SET(inet, ONE_PARAM_(CassInet value), value)
CASS_USER_TYPE_SET(uuid, ONE_PARAM_(CassUuid value), value)
CASS_USER_TYPE_SET(collection, ONE_PARAM_(const CassCollection* value), value)
CASS_USER_TYPE_SET(user_type, ONE_PARAM_(const CassUserType* value), value)
CASS_USER_TYPE_SET(bytes,
                   TWO_PARAMS_(const cass_byte_t* value, size_t value_size),
                   cass::CassBytes(value, value_size))
CASS_USER_TYPE_SET(decimal,
                   THREE_PARAMS_(const cass_byte_t* varint, size_t varint_size, int scale),
                   cass::CassDecimal(varint, varint_size, scale))

#undef CASS_USER_TYPE_SET

CassError cass_user_type_set_string(CassUserType* user_type,
                                    size_t index,
                                    const char* value) {
  return user_type->set(index, cass::CassString(value, strlen(value)));
}

CassError cass_user_type_set_string_n(CassUserType* user_type,
                                      size_t index,
                                      const char* value,
                                      size_t value_length) {
  return user_type->set(index, cass::CassString(value, value_length));
}

CassError cass_user_type_set_string_by_name(CassUserType* user_type,
                                            const char* name,
                                            const char* value) {
  return cass::bind_by_name(user_type,
                            cass::StringRef(name),
                            cass::CassString(value, strlen(value)));
}

CassError cass_user_type_set_string_by_name_n(CassUserType* user_type,
                                              const char* name,
                                              size_t name_length,
                                              const char* value,
                                              size_t value_length) {
  return cass::bind_by_name(user_type,
                            cass::StringRef(name, name_length),
                            cass::CassString(value, value_length));
}
#endif

void cass_user_type_free(CassUserType* user_type) {
  delete user_type->from();
}

} // extern "C"


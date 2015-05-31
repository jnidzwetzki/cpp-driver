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

#include "data_type.hpp"

#include "collection.hpp"
#include "types.hpp"
#include "user_type_value.hpp"

namespace cass {

const SharedRefPtr<DataType> DataType::NIL;

bool cass::IsValidDataType<const Collection*>::operator()(const Collection* value,
                                                          const SharedRefPtr<DataType>& data_type) const {
  return value->data_type()->equals(data_type, false);
}

SharedRefPtr<DataType> cass::CreateDataType<const Collection*>::operator()(const Collection* value) const {
  return value->data_type();
}

bool cass::IsValidDataType<const UserTypeValue*>::operator()(const UserTypeValue* value,
                                                             const SharedRefPtr<DataType>& data_type) const {
  return value->user_type()->equals(data_type, true);
}

SharedRefPtr<DataType> cass::CreateDataType<const UserTypeValue*>::operator()(const UserTypeValue* value) const {
  return value->user_type();
}

} // namespace cass
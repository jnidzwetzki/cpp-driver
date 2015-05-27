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

#ifndef __CASS_MACROS_HPP_INCLUDED__
#define __CASS_MACROS_HPP_INCLUDED__

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  TypeName& operator=(const TypeName&)

#define UNUSED_(X) ((void)X)

#define ONE_PARAM1_(A) ,A
#define ONE_PARAM_(A) ONE_PARAM1_(A)

#define TWO_PARAMS1_(A, B)  ,A,B
#define TWO_PARAMS_(A, B) TWO_PARAMS1_(A, B)

#define THREE_PARAMS1_(A, B, C)  ,A,B,C
#define THREE_PARAMS_(A, B, C) THREE_PARAMS1_(A, B, C)

#endif




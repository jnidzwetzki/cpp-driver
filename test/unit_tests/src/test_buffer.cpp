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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "buffer.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(buffer)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::Buffer buf(sizeof(int32_t));
  buf.encode_int32(0, 16);
  BOOST_CHECK(buf.ref()->ref_count() == 1);
}

BOOST_AUTO_TEST_CASE(copy)
{
  cass::Buffer buf1(sizeof(int32_t));

  {
    cass::Buffer buf2(buf1);
    BOOST_CHECK(buf1.base == buf2.base);
    BOOST_CHECK(buf1.ref()->ref_count() == 2);
  }

  BOOST_CHECK(buf1.ref()->ref_count() == 1);
}

BOOST_AUTO_TEST_CASE(null_copy)
{
  cass::Buffer buf1;
  cass::Buffer buf2(buf1);
}

BOOST_AUTO_TEST_CASE(equals)
{
  cass::Buffer buf1(sizeof(int32_t));

  {
    cass::Buffer buf2;
    buf2 = buf1;
    BOOST_CHECK(buf1.base == buf2.base);
    BOOST_CHECK(buf1.ref()->ref_count() == 2);
  }

  BOOST_CHECK(buf1.ref()->ref_count() == 1);
}


BOOST_AUTO_TEST_SUITE_END()

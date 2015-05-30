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

#ifndef __CASS_STATEMENT_HPP_INCLUDED__
#define __CASS_STATEMENT_HPP_INCLUDED__

#include "abstract_data.hpp"
#include "constants.hpp"
#include "macros.hpp"
#include "request.hpp"
#include "result_metadata.hpp"
#include "result_response.hpp"

#include <vector>
#include <string>

namespace cass {

class Statement : public RoutableRequest, public AbstractData {
public:
  struct NamedParameter : HashIndex::Entry {
    NamedParameter(const std::string& name)
      : buf(sizeof(uint16_t) + name.size()) {
      buf.encode_string(0, name.data(), name.size());
    }

    StringRef to_string_ref() const {
      return StringRef(buf.data() + sizeof(uint16_t),
                       buf.size() - sizeof(uint16_t));
    }

    Buffer buf;
  };

  typedef std::vector<NamedParameter> NamedParameterVec;

  Statement(uint8_t opcode, uint8_t kind, size_t values_count = 0)
      : RoutableRequest(opcode)
      , AbstractData(values_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind) { }

  Statement(uint8_t opcode, uint8_t kind, size_t values_count,
            const std::vector<size_t>& key_indices,
            const std::string& keyspace)
      : RoutableRequest(opcode, keyspace)
      , AbstractData(values_count)
      , skip_metadata_(false)
      , page_size_(-1)
      , kind_(kind)
      , key_indices_(key_indices) { }

  virtual ~Statement() { }

  bool skip_metadata() const { return skip_metadata_; }

  void set_skip_metadata(bool skip_metadata) {
    skip_metadata_ = skip_metadata;
  }

  int32_t page_size() const { return page_size_; }

  void set_page_size(int32_t page_size) { page_size_ = page_size; }

  const std::string paging_state() const { return paging_state_; }

  void set_paging_state(const std::string& paging_state) {
    paging_state_ = paging_state;
  }

  uint8_t kind() const { return kind_; }

  virtual const std::string& query() const = 0;

  const NamedParameterVec& named_params() const {
    return named_params_;
  }

  size_t named_params_count() const{
    return named_params_.size();
  }

  void add_key_index(size_t index) { key_indices_.push_back(index); }

  virtual bool get_routing_key(std::string* routing_key) const;

  int32_t copy_buffers(BufferVec* bufs) const;

  int32_t copy_buffers_with_names(BufferVec* bufs) const;

protected:
  size_t get_named_indices(StringRef name, HashIndex::IndexVec* indices);

private:
  bool skip_metadata_;
  int32_t page_size_;
  std::string paging_state_;
  uint8_t kind_;
  std::vector<size_t> key_indices_;

private:
  NamedParameterVec named_params_;
  ScopedPtr<HashIndex> named_params_index_;

private:
  DISALLOW_COPY_AND_ASSIGN(Statement);
};

} // namespace cass

#endif

/*
  Copyright 2014 DataStax

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

#ifndef __CASS_PREPARE_HANDLER_HPP_INCLUDED__
#define __CASS_PREPARE_HANDLER_HPP_INCLUDED__

#include "response_callback.hpp"
#include "request_handler.hpp"
#include "message.hpp"
#include "prepare.hpp"
#include "io_worker.hpp"

namespace cass {

class PrepareHandler : public ResponseCallback {
  public:
    PrepareHandler(RetryCallback retry_callback,
                   RequestHandler* request_handler)
      : retry_callback_(retry_callback)
      , request_(new Message())
      , request_handler_(request_handler) { }

    bool init() {
      Prepare* prepare = new Prepare();
      request_->opcode = prepare->opcode();
      request_->body.reset(prepare);
      if(request_handler_->request()->opcode == CQL_OPCODE_EXECUTE) {
        Bound* bound = static_cast<Bound*>(request_handler_->request()->body.get());
        prepare->prepare_string(bound->prepared_statement);
        return true;
      }
      return false; // Invalid request type
    }

    virtual Message* request() const {
      return request_.get();
    }

    virtual void on_set(Message* response) {
      switch(response->opcode) {
        case CQL_OPCODE_RESULT: {
          Result* result = static_cast<Result*>(response->body.get());
          if(result->kind == CASS_RESULT_KIND_PREPARED) {
            retry_callback_(request_handler_.release(), RETRY_WITH_CURRENT_HOST);
          } else {
            retry_callback_(request_handler_.release(), RETRY_WITH_NEXT_HOST);
          }
        }
          break;
        case CQL_OPCODE_ERROR:
          retry_callback_(request_handler_.release(), RETRY_WITH_NEXT_HOST);
          break;
        default:
          break;
      }
    }

    virtual void on_error(CassError code, const std::string& message) {
      retry_callback_(request_handler_.release(), RETRY_WITH_NEXT_HOST);
    }

    virtual void on_timeout() {
      retry_callback_(request_handler_.release(), RETRY_WITH_NEXT_HOST);
    }

  private:
    RetryCallback retry_callback_;
    std::unique_ptr<Message> request_;
    std::unique_ptr<RequestHandler> request_handler_;
};

} // namespace cass

#endif

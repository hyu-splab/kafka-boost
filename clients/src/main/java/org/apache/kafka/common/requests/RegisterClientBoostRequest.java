/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.RegisterClientBoostRequestData;
import org.apache.kafka.common.message.RegisterClientBoostResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class RegisterClientBoostRequest extends AbstractRequest {

  public static class Builder extends AbstractRequest.Builder<RegisterClientBoostRequest> {
    private final RegisterClientBoostRequestData data;

    public Builder(RegisterClientBoostRequestData data) {
      super(ApiKeys.REGISTER_CLIENT_BOOST);
      this.data = data;
    }

    @Override
    public RegisterClientBoostRequest build(short version) {
      return new RegisterClientBoostRequest(data, version);
    }
  }

  private final RegisterClientBoostRequestData data;

  public RegisterClientBoostRequest(RegisterClientBoostRequestData data, short version) {
    super(ApiKeys.REGISTER_CLIENT_BOOST, version);
    this.data = data;
  }

  @Override
  public RegisterClientBoostRequestData data() {
    return data;
  }

  @Override
  public RegisterClientBoostResponse getErrorResponse(int throttleTimeMs, Throwable e) {
    Errors error = Errors.forException(e);
    return new RegisterClientBoostResponse(new RegisterClientBoostResponseData()
        .setThrottleTimeMs(throttleTimeMs)
        .setErrorCode(error.code())
        .setErrorMessage(e.getMessage()));
  }

  public static RegisterClientBoostRequest parse(Readable readable, short version) {
    return new RegisterClientBoostRequest(new RegisterClientBoostRequestData(readable, version),
        version);
  }
}

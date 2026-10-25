/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.websocket.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum WebSocketConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECT_FAILED("WEBSOCKET-01", "Cannot connect to websocket server"),
    CONFIG_VALIDATION_FAILED("WEBSOCKET-02", "The config validation is failed"),
    SEND_MESSAGE_FAILED("WEBSOCKET-03", "Failed to send message to websocket server"),
    DESERIALIZE_FAILED("WEBSOCKET-04", "Failed to deserialize the received message"),
    UNSUPPORTED_DATA_FORMAT("WEBSOCKET-05", "The data format is unsupported");

    private final String code;

    private final String description;

    WebSocketConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getErrorMessage() {
        return SeaTunnelErrorCode.super.getErrorMessage();
    }
}

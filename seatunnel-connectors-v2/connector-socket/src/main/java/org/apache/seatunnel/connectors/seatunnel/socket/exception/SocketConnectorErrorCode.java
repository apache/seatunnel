/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.socket.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SocketConnectorErrorCode implements SeaTunnelErrorCode {

    SOCKET_SERVER_CONNECT_FAILED("SOCKET-01", "Cannot connect to socket server"),
    SEND_MESSAGE_TO_SOCKET_SERVER_FAILED("SOCKET-02", "Failed to send message to socket server"),
    SOCKET_WRITE_FAILED("SOCKET-03", "Unable to write; interrupted while doing another attempt");

    private final String code;

    private final String description;

    SocketConnectorErrorCode(String code, String description) {
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

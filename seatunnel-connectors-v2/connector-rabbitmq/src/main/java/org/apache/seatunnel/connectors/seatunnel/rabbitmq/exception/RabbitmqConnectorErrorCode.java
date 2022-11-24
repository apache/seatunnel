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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum RabbitmqConnectorErrorCode implements SeaTunnelErrorCode {
    HANDLE_SHUTDOWN_SIGNAL_FAILED("RABBITMQ-01", "handle queue consumer shutdown signal failed"),
    CREATE_RABBITMQ_CLIENT_FAILED("RABBITMQ-02", "create rabbitmq client failed"),
    CLOSE_CONNECTION_FAILED("RABBITMQ-03", "close connection failed"),
    SEND_MESSAGE_FAILED("RABBITMQ-04", "send messages failed"),
    MESSAGE_ACK_FAILED("RABBITMQ-05", "messages could not be acknowledged during checkpoint creation"),
    MESSAGE_ACK_REJECTED("RABBITMQ-06", "messages could not be acknowledged with basicReject"),
    PARSE_URI_FAILED("RABBITMQ-07", "parse uri failed"),
    INIT_SSL_CONTEXT_FAILED("RABBITMQ-08", "initialize ssl context failed"),
    SETUP_SSL_FACTORY_FAILED("RABBITMQ-09", "setup ssl factory failed");

    private final String code;
    private final String description;

    RabbitmqConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

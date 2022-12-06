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

package org.apache.seatunnel.connectors.seatunnel.pulsar.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum PulsarConnectorErrorCode implements SeaTunnelErrorCode {

    OPEN_PULSAR_ADMIN_FAILED("PULSAR-01", "Open pulsar admin failed"),
    OPEN_PULSAR_CLIENT_FAILED("PULSAR-02", "Open pulsar client failed"),
    PULSAR_AUTHENTICATION_FAILED("PULSAR-03", "Pulsar authentication failed"),
    SUBSCRIBE_TOPIC_FAILED("PULSAR-04", "Subscribe topic from pulsar failed"),
    GET_LAST_CURSOR_FAILED("PULSAR-05", "Get last cursor of pulsar topic failed"),
    GET_TOPIC_PARTITION_FAILED("PULSAR-06", "Get partition information of pulsar topic failed");

    private final String code;
    private final String description;

    PulsarConnectorErrorCode(String code, String description) {
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

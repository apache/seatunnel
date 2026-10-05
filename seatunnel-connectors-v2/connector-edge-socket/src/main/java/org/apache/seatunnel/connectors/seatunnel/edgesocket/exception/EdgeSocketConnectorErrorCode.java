/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.edgesocket.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum EdgeSocketConnectorErrorCode implements SeaTunnelErrorCode {
    SOURCE_BIND_FAILED("EDGE_SOCKET-01", "Failed to bind edge socket ingress port"),
    SOURCE_REOPEN_EXHAUSTED("EDGE_SOCKET-02", "Reopen retries exhausted"),
    SOURCE_ACCEPT_ERROR("EDGE_SOCKET-03", "Failed to accept edge collector connection"),
    SOURCE_READ_ERROR("EDGE_SOCKET-04", "Failed to read data from edge collector socket"),
    PACKET_DECODE_ERROR("EDGE_SOCKET-05", "Failed to decode ingress packet"),
    PACKET_UNSUPPORTED_COMPRESSION("EDGE_SOCKET-06", "Unsupported ingress packet compression type"),
    PACKET_UNSUPPORTED_ENCRYPTION("EDGE_SOCKET-07", "Unsupported ingress packet encryption type"),
    PACKET_AES_KEY_MISSING("EDGE_SOCKET-08", "Missing secret_key for AES_GCM packet"),
    COLLECTOR_REJECTED(
            "EDGE_SOCKET-09",
            "Rejected collector connection because another collector is already connected");

    private final String code;
    private final String description;

    EdgeSocketConnectorErrorCode(String code, String description) {
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

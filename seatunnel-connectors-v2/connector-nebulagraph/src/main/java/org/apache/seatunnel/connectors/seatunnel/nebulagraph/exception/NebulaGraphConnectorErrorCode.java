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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum NebulaGraphConnectorErrorCode implements SeaTunnelErrorCode {
    INVALID_CONFIG("NEBULAGRAPH-01", "NebulaGraph connector configuration is invalid"),
    UNSUPPORTED_DATA_TYPE("NEBULAGRAPH-02", "SeaTunnel data type is not supported"),
    CONNECT_FAILED("NEBULAGRAPH-03", "Failed to connect to NebulaGraph"),
    WRITE_FAILED("NEBULAGRAPH-04", "Failed to write to NebulaGraph");

    private final String code;
    private final String description;

    NebulaGraphConnectorErrorCode(String code, String description) {
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

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

package org.apache.seatunnel.connectors.seatunnel.databend.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum DatabendConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECT_FAILED("DATABEND-01", "Failed to connect to Databend"),
    SQL_OPERATION_FAILED("DATABEND-02", "Failed to execute SQL in Databend"),
    PARSE_RESPONSE_FAILED("DATABEND-03", "Failed to parse data from Databend"),
    GENERATE_SQL_FAILED("DATABEND-04", "Failed to generate SQL for Databend"),
    DRIVER_NOT_FOUND("DATABEND-05", "Failed to get driver"),
    UNSUPPORTED_DATA_TYPE("DATABEND-06", "unsupported data type"),
    ILLEGAL_STATE("DATABEND-07", "illegal state"),
    SCHEMA_NOT_FOUND(10001, "Schema not found"),
    SCHEMA_MISMATCH(10002, "Schema mismatch");

    private final String code;
    private final String description;

    DatabendConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    DatabendConnectorErrorCode(int code, String description) {
        this.code = "DATABEND-" + code;
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

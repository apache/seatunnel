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

package org.apache.seatunnel.connectors.doris.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum DorisConnectorErrorCode implements SeaTunnelErrorCode {
    STREAM_LOAD_FAILED("Doris-01", "stream load error"),
    COMMIT_FAILED("Doris-02", "commit error"),
    REST_SERVICE_FAILED("Doris-03", "rest service error"),
    ROUTING_FAILED("Doris-04", "routing error"),
    ARROW_READ_FAILED("Doris-05", "arrow read error"),
    BACKEND_CLIENT_FAILED("Doris-06", "backend client error"),
    ROW_BATCH_GET_FAILED("Doris-07", "row batch get error"),
    SCHEMA_FAILED("Doirs-08", "get schema error"),
    SCAN_BATCH_FAILED("Doris-09", "scan batch error"),
    RESOURCE_CLOSE_FAILED("Doris-10", "resource close failed"),
    SCHEMA_CHANGE_FAILED("Doris-11", "schema change failed"),
    SHOULD_NEVER_HAPPEN("Doris-00", "Should Never Happen !");

    private final String code;
    private final String description;

    DorisConnectorErrorCode(String code, String description) {
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

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

package org.apache.seatunnel.connectors.seatunnel.tdengine.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TDengineConnectorErrorCode implements SeaTunnelErrorCode {
    SQL_OPERATION_FAILED("TDengine-01", "execute sql failed"),
    CONNECTION_FAILED("TDengine-02", "connection operation failed"),
    TYPE_MAPPER_FAILED("TDengine-03", "type mapping failed"),
    READER_FAILED("TDengine-04", "reader operation failed"),
    WRITER_FAILED("TDengine-05", "writer operation failed");

    private final String code;
    private final String description;

    TDengineConnectorErrorCode(String code, String description) {
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

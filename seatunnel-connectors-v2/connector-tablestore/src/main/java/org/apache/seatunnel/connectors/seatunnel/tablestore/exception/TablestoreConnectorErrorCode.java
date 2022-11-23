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

package org.apache.seatunnel.connectors.seatunnel.tablestore.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum TablestoreConnectorErrorCode implements SeaTunnelErrorCode {
    FLUSH_DATA_FAILED("TABLESTORE-01", "Writing items to Tablestore failed"),
    WRITE_ROW_FAILED("TABLESTORE-02", "Failed to send this row of data"),
    UNSUPPORTED_PRIMARY_KEY_TYPE("TABLESTORE-03", "Unsupported primaryKeyType"),
    UNSUPPORTED_COLUMN_TYPE("TABLESTORE-04", "Unsupported columnType");

    private final String code;

    private final String description;

    TablestoreConnectorErrorCode(String code, String description) {
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

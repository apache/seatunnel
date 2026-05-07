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

package org.apache.seatunnel.connectors.seatunnel.bigtable.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum BigtableConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECTION_FAILED("Bigtable-01", "Build Bigtable connection failed"),
    TABLE_NOT_FOUND("Bigtable-02", "Bigtable table not found"),
    TABLE_CREATE_FAILED("Bigtable-03", "Bigtable table create failed"),
    TABLE_DELETE_FAILED("Bigtable-04", "Bigtable table delete failed"),
    TABLE_TRUNCATE_FAILED("Bigtable-05", "Bigtable table truncate failed"),
    TABLE_QUERY_FAILED("Bigtable-06", "Bigtable table query failed"),
    WRITE_FAILED("Bigtable-07", "Bigtable write failed"),
    READ_FAILED("Bigtable-08", "Bigtable read failed"),
    CREDENTIALS_FAILED("Bigtable-09", "Failed to load Bigtable credentials"),
    ;

    private final String code;
    private final String description;

    BigtableConnectorErrorCode(String code, String description) {
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

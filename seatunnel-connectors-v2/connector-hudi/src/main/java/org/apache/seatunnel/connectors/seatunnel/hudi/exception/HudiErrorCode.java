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

package org.apache.seatunnel.connectors.seatunnel.hudi.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum HudiErrorCode implements SeaTunnelErrorCode {
    CANNOT_FIND_PARQUET_FILE(
            "HUDI-01",
            "Hudi connector can not find parquet file in table path '<tablePath>', please check!"),
    FLUSH_DATA_FAILED("HUDI-02", "Flush data operation that in hudi sink connector failed"),
    UNSUPPORTED_OPERATION("HUDI-03", "Unsupported operation"),
    TABLE_CONFIG_NOT_FOUND("HUDI-04", "Table configuration not set."),
    INITIALIZE_TABLE_FAILED("HUDI-05", "Initialize table failed"),
    ;

    private final String code;
    private final String description;

    HudiErrorCode(String code, String description) {
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
}

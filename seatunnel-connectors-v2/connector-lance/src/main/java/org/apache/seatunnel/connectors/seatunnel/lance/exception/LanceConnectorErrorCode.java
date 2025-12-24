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
package org.apache.seatunnel.connectors.seatunnel.lance.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum LanceConnectorErrorCode implements SeaTunnelErrorCode {
    TABLE_EXISTS_EXCEPTION("LANCE-01", "Table Exists response exception"),

    TABLE_JSON_ARROW_SCHEMA_CONVERT_EXCEPTION(
            "LANCE-02", "Table JsonArrowSchema convert exception"),

    TABLE_DATASET_PATH_OPEN_EXCEPTION("LANCE-03", "DataSet path open exception"),

    TABLE_DATASET_WRITE_ST_ROW_EXCEPTION("LANCE-04", "Dataset write seatunnelRow exception");

    private final String code;
    private final String description;

    LanceConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    };

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}

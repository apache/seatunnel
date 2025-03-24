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

package org.apache.seatunnel.api.common;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SeaTunnelAPIErrorCode implements SeaTunnelErrorCode {
    CONFIG_VALIDATION_FAILED("API-01", "Configuration item validate failed"),
    OPTION_VALIDATION_FAILED("API-02", "Option item validate failed"),
    CATALOG_INITIALIZE_FAILED("API-03", "Catalog initialize failed"),
    DATABASE_NOT_EXISTED("API-04", "Database not existed"),
    TABLE_NOT_EXISTED("API-05", "Table not existed"),
    FACTORY_INITIALIZE_FAILED("API-06", "Factory initialize failed"),
    DATABASE_ALREADY_EXISTED("API-07", "Database already existed"),
    TABLE_ALREADY_EXISTED("API-08", "Table already existed"),
    HANDLE_SAVE_MODE_FAILED("API-09", "Handle save mode failed"),
    SOURCE_ALREADY_HAS_DATA("API-10", "The target data source already has data"),
    SINK_TABLE_NOT_EXIST("API-11", "The sink table not exist"),
    LIST_DATABASES_FAILED("API-12", "List databases failed"),
    LIST_TABLES_FAILED("API-13", "List tables failed"),
    GET_PRIMARY_KEY_FAILED("API-14", "Get primary key failed");

    private final String code;
    private final String description;

    SeaTunnelAPIErrorCode(String code, String description) {
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

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
    CONFIG_VALIDATION_FAILED("API-01", "Configuration item validation failure"),
    OPTION_VALIDATION_FAILED("API-02", "Option item validation failure"),
    CATALOG_ERROR("API-03", "Catalog related failure"),
    DATABASE_NOT_FOUND("API-04", "Database not found"),
    TABLE_NOT_FOUND("API-05", "Table not found"),
    FACTORY_INITIALIZATION_FAILED("API-06", "Factory initialization failure"),;

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

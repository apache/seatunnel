/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.easysearch.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum EasysearchConnectorErrorCode implements SeaTunnelErrorCode {
    UNSUPPORTED_OPERATION("EASYSEARCH-COMMON-01", "Unsupported operation"),
    JSON_OPERATION_FAILED("EASYSEARCH-COMMON-02", "Json covert/parse operation failed"),
    SQL_OPERATION_FAILED(
            "EASYSEARCH-COMMON-04",
            "Sql operation failed, such as (execute,addBatch,close) etc..."),
    UNSUPPORTED_DATA_TYPE("EASYSEARCH-COMMON-03", "Unsupported data type"),
    BULK_RESPONSE_ERROR("EASYSEARCH-01", "Bulk ezs response error"),
    GET_EZS_VERSION_FAILED("EASYSEARCH-02", "Get easysearch version failed"),
    SCROLL_REQUEST_ERROR("EASYSEARCH-03", "Fail to scroll request"),
    GET_INDEX_DOCS_COUNT_FAILED("EASYSEARCH-04", "Get easysearch document index count failed"),
    LIST_INDEX_FAILED("EASYSEARCH-05", "List easysearch index failed"),
    DROP_INDEX_FAILED("EASYSEARCH-06", "Drop easysearch index failed"),
    CREATE_INDEX_FAILED("EASYSEARCH-07", "Create easysearch index failed"),
    EZS_FIELD_TYPE_NOT_SUPPORT("EASYSEARCH-08", "Not support the easysearch field type");

    private final String code;
    private final String description;

    EasysearchConnectorErrorCode(String code, String description) {
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

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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum ElasticsearchConnectorErrorCode implements SeaTunnelErrorCode {
    BULK_RESPONSE_ERROR("ELASTICSEARCH-01", "Bulk es response error"),
    GET_ES_VERSION_FAILED("ELASTICSEARCH-02", "Get elasticsearch version failed"),
    SCROLL_REQUEST_ERROR("ELASTICSEARCH-03", "Fail to scroll request"),
    GET_INDEX_DOCS_COUNT_FAILED(
            "ELASTICSEARCH-04", "Get elasticsearch document index count failed"),
    LIST_INDEX_FAILED("ELASTICSEARCH-05", "List elasticsearch index failed"),
    DROP_INDEX_FAILED("ELASTICSEARCH-06", "Drop elasticsearch index failed"),
    CREATE_INDEX_FAILED("ELASTICSEARCH-07", "Create elasticsearch index failed"),
    ES_FIELD_TYPE_NOT_SUPPORT("ELASTICSEARCH-08", "Not support the elasticsearch field type"),
    CLEAR_INDEX_DATA_FAILED("ELASTICSEARCH-09", "Clear elasticsearch index data failed"),
    CHECK_INDEX_FAILED("ELASTICSEARCH-10", "Failed to check whether the index exists"),
    SOURCE_CONFIG_ERROR_01(
            "ELASTICSEARCH-11",
            "'index' or 'index_list' must be configured, with at least one being required."),
    ADD_FIELD_FAILED("ELASTICSEARCH-12", "Field add failed"),
    SCHEMA_CHANGE_FAILED("ELASTICSEARCH-13", "Schema change failed"),
    ;

    private final String code;
    private final String description;

    ElasticsearchConnectorErrorCode(String code, String description) {
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

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
package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum JsonPathTransformErrorCode implements SeaTunnelErrorCode {
    COLUMNS_MUST_NOT_EMPTY(
            "JSONPATH_ERROR_CODE-01", "JsonPathTransform config columns must not empty"),
    SRC_FIELD_MUST_NOT_EMPTY(
            "JSONPATH_ERROR_CODE-02", "JsonPathTransform src_field must not empty"),
    PATH_MUST_NOT_EMPTY(
            "JSONPATH_ERROR_CODE-03", "JsonPathTransform config field path must not empty"),
    DEST_FIELD_MUST_NOT_EMPTY(
            "JSONPATH_ERROR_CODE-04", "JsonPathTransform dest_field must not empty"),

    JSON_PATH_COMPILE_ERROR("JSONPATH_ERROR_CODE-05", "JsonPathTransform path is invalid"),
    DEST_TYPE_MUST_NOT_EMPTY(
            "JSONPATH_ERROR_CODE-06", "JsonPathTransform dest_type must not empty"),
    SRC_FIELD_NOT_FOUND(
            "JSONPATH_ERROR_CODE-02", "JsonPathTransform src_field not found in source"),
    ;
    private final String code;
    private final String description;

    JsonPathTransformErrorCode(String code, String description) {
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

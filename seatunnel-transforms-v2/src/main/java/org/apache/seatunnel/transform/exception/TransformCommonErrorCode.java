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

public enum TransformCommonErrorCode implements SeaTunnelErrorCode {
    INPUT_FIELD_NOT_FOUND(
            "TRANSFORM_COMMON-01",
            "The input field '<field>' of '<transform>' transform not found in upstream schema"),
    INPUT_FIELDS_NOT_FOUND(
            "TRANSFORM_COMMON-02",
            "The input fields '<fields>' of '<transform>' transform not found in upstream schema"),
    INPUT_TABLE_NOT_FOUND(
            "TRANSFORM_COMMON-03",
            "The input table '<table>' of '<transform>' transform not found in upstream schema"),
    INPUT_TABLE_FIELD_NOT_FOUND(
            "TRANSFORM_COMMON-04",
            "The input field '<field>'of table '<table>' of '<transform>' transform not found in upstream schema"),
    GET_CATALOG_TABLE_WITH_NOT_EXIST_FIELDS_ERROR(
            "TRANSFORM_COMMON-05",
            "The '<transform>' upstream schema not exist fields: '<tableNotExistedFields>'"),
    GET_CATALOG_TABLE_WITH_NOT_EXIST_TABLES_ERROR(
            "TRANSFORM_COMMON-06", "The '<transform>' upstream schema not exist tables '<tables>'"),
    GET_CATALOG_TABLES_WITH_NOT_EXIST_FIELDS_AND_TABLES_ERROR(
            "TRANSFORM_COMMON-07",
            "The '<transform>' upstream schema not exist table '<tables>'，upstream schema not exist fields: '<tableNotExistedFields>'");

    private final String code;
    private final String description;

    TransformCommonErrorCode(String code, String description) {
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

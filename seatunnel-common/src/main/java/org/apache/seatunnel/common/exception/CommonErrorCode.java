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

package org.apache.seatunnel.common.exception;

/** SeaTunnel connector error code interface */
public enum CommonErrorCode implements SeaTunnelErrorCode {
    FILE_OPERATION_FAILED("COMMON-01", "<identifier> <operation> file '<fileName>' failed."),
    JSON_OPERATION_FAILED(
            "COMMON-02", "<identifier> JSON convert/parse '<payload>' operation failed."),
    UNSUPPORTED_DATA_TYPE(
            "COMMON-07", "'<identifier>' unsupported data type '<dataType>' of '<field>'"),
    UNSUPPORTED_ENCODING("COMMON-08", "unsupported encoding '<encoding>'"),
    CONVERT_TO_SEATUNNEL_TYPE_ERROR(
            "COMMON-16",
            "'<connector>' <type> unsupported convert type '<dataType>' of '<field>' to SeaTunnel data type."),
    CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE(
            "COMMON-17",
            "'<identifier>' unsupported convert type '<dataType>' of '<field>' to SeaTunnel data type."),
    CONVERT_TO_CONNECTOR_TYPE_ERROR(
            "COMMON-18",
            "'<connector>' <type> unsupported convert SeaTunnel data type '<dataType>' of '<field>' to connector data type."),
    CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE(
            "COMMON-19",
            "'<identifier>' unsupported convert SeaTunnel data type '<dataType>' of '<field>' to connector data type."),
    GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR(
            "COMMON-20",
            "'<catalogName>' table '<tableName>' unsupported get catalog table with field data types '<fieldWithDataTypes>'"),
    GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR(
            "COMMON-21",
            "'<catalogName>' tables unsupported get catalog table，the corresponding field types in the following tables are not supported: '<tableUnsupportedTypes>'"),
    FILE_NOT_EXISTED(
            "COMMON-22",
            "<identifier> <operation> file '<fileName>' failed, because it not existed."),
    WRITE_SEATUNNEL_ROW_ERROR(
            "COMMON-23",
            "<connector> write SeaTunnelRow failed, the SeaTunnelRow value is '<seaTunnelRow>'.");

    private final String code;
    private final String description;

    CommonErrorCode(String code, String description) {
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

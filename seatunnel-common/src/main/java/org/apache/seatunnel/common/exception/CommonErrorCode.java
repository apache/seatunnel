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
            "<connector> write SeaTunnelRow failed, the SeaTunnelRow value is '<seaTunnelRow>'."),
    SQL_TEMPLATE_HANDLED_ERROR(
            "COMMON-24",
            "The table of <tableName> has no <keyName>, but the template \n <template> \n which has the place holder named <placeholder>. Please use the option named <optionName> to specify sql template"),
    VERSION_NOT_SUPPORTED("COMMON-25", "<identifier> <version> is unsupported."),
    OPERATION_NOT_SUPPORTED("COMMON-26", "<identifier> <operation> is unsupported."),
    CONVERT_TO_SEATUNNEL_PROPS_BLANK_ERROR(
            "COMMON-27", "The props named '<props>' of '<connector>' is blank."),
    UNSUPPORTED_ARRAY_GENERIC_TYPE(
            "COMMON-28",
            "'<identifier>' array type not support genericType '<genericType>' of '<fieldName>'"),
    UNSUPPORTED_ROW_KIND(
            "COMMON-29", "'<identifier>' table '<tableId>' not support rowKind  '<rowKind>'"),

    WRITE_SEATUNNEL_ROW_ERROR_WITH_SCHEMA_INCOMPATIBLE_SCHEMA(
            "COMMON-30",
            "<connector>: The source filed with schema '<sourceFieldSqlSchema>', except filed schema of sink is '<exceptFieldSqlSchema>'; but the filed in sink table which actual schema is '<sinkFieldSqlSchema>'. Please check schema of sink table."),

    WRITE_SEATUNNEL_ROW_ERROR_WITH_FILEDS_NOT_MATCH(
            "COMMON-31",
            "<connector>: The source has '<sourceFieldsNum>' fields, but the table of sink has '<sinkFieldsNum>' fields. Please check schema of sink table."),
    FORMAT_DATE_ERROR(
            "COMMON-32",
            "The date format '<date>' of field '<field>' is not supported. Please check the date format."),
    FORMAT_DATETIME_ERROR(
            "COMMON-33",
            "The datetime format '<datetime>' of field '<field>' is not supported. Please check the datetime format."),
    UNSUPPORTED_METHOD("COMMON-34", "'<identifier>' unsupported the method '<methodName>'"),
    KERBEROS_AUTHORIZED_FAILED("COMMON-35", "Kerberos authorized failed"),
    ;

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

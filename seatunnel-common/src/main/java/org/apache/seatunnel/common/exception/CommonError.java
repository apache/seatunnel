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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.constants.PluginType;

import org.apache.commons.collections4.map.SingletonMap;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_PROPS_BLANK_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.FILE_NOT_EXISTED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.FILE_OPERATION_FAILED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.JSON_OPERATION_FAILED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.OPERATION_NOT_SUPPORTED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.SQL_TEMPLATE_HANDLED_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_ARRAY_GENERIC_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_ENCODING;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_ROW_KIND;
import static org.apache.seatunnel.common.exception.CommonErrorCode.VERSION_NOT_SUPPORTED;
import static org.apache.seatunnel.common.exception.CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR;
import static org.apache.seatunnel.common.exception.CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR_WITH_FILEDS_NOT_MATCH;
import static org.apache.seatunnel.common.exception.CommonErrorCode.WRITE_SEATUNNEL_ROW_ERROR_WITH_SCHEMA_INCOMPATIBLE_SCHEMA;

/**
 * The common error of SeaTunnel. This is an alternative to {@link CommonErrorCodeDeprecated} and is
 * used to define non-bug errors or expected errors for all connectors and engines. We need to
 * define a corresponding enumeration type in {@link CommonErrorCode} to determine the output error
 * message format and content. Then define the corresponding method in {@link CommonError} to
 * construct the corresponding error instance.
 */
public class CommonError {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static SeaTunnelRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_OPERATION_FAILED, params, cause);
    }

    public static SeaTunnelRuntimeException fileOperationFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_OPERATION_FAILED, params);
    }

    public static SeaTunnelRuntimeException fileNotExistFailed(
            String identifier, String operation, String fileName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        params.put("fileName", fileName);
        return new SeaTunnelRuntimeException(FILE_NOT_EXISTED, params);
    }

    public static SeaTunnelRuntimeException writeSeaTunnelRowFailed(
            String connector, String row, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("seaTunnelRow", row);
        return new SeaTunnelRuntimeException(WRITE_SEATUNNEL_ROW_ERROR, params, cause);
    }

    public static SeaTunnelRuntimeException unsupportedDataType(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(UNSUPPORTED_DATA_TYPE, params);
    }

    public static SeaTunnelRuntimeException unsupportedVersion(String identifier, String version) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("version", version);
        return new SeaTunnelRuntimeException(VERSION_NOT_SUPPORTED, params);
    }

    public static SeaTunnelRuntimeException unsupportedEncoding(String encoding) {
        Map<String, String> params = new SingletonMap<>("encoding", encoding);
        return new SeaTunnelRuntimeException(UNSUPPORTED_ENCODING, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToSeaTunnelTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String connector, PluginType pluginType, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("type", pluginType.getType());
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorPropsBlankError(
            String connector, String props) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("props", props);
        return new SeaTunnelRuntimeException(CONVERT_TO_SEATUNNEL_PROPS_BLANK_ERROR, params);
    }

    public static SeaTunnelRuntimeException convertToConnectorTypeError(
            String identifier, String dataType, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE, params);
    }

    public static SeaTunnelRuntimeException getCatalogTableWithUnsupportedType(
            String catalogName, String tableName, Map<String, String> fieldWithDataTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        params.put("tableName", tableName);
        try {
            params.put("fieldWithDataTypes", OBJECT_MAPPER.writeValueAsString(fieldWithDataTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRuntimeException(GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException getCatalogTablesWithUnsupportedType(
            String catalogName, Map<String, Map<String, String>> tableUnsupportedTypes) {
        Map<String, String> params = new HashMap<>();
        params.put("catalogName", catalogName);
        try {
            params.put(
                    "tableUnsupportedTypes",
                    OBJECT_MAPPER.writeValueAsString(tableUnsupportedTypes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRuntimeException(
                GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR, params);
    }

    public static SeaTunnelRuntimeException jsonOperationError(String identifier, String payload) {
        return jsonOperationError(identifier, payload, null);
    }

    public static SeaTunnelRuntimeException jsonOperationError(
            String identifier, String payload, Throwable cause) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("payload", payload);
        SeaTunnelErrorCode code = JSON_OPERATION_FAILED;

        if (cause != null) {
            return new SeaTunnelRuntimeException(code, params, cause);
        } else {
            return new SeaTunnelRuntimeException(code, params);
        }
    }

    public static SeaTunnelRuntimeException unsupportedOperation(
            String identifier, String operation) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("operation", operation);
        return new SeaTunnelRuntimeException(OPERATION_NOT_SUPPORTED, params);
    }

    public static SeaTunnelRuntimeException sqlTemplateHandledError(
            String tableName,
            String keyName,
            String template,
            String placeholder,
            String optionName) {
        Map<String, String> params = new HashMap<>();
        params.put("tableName", tableName);
        params.put("keyName", keyName);
        params.put("template", template);
        params.put("placeholder", placeholder);
        params.put("optionName", optionName);
        return new SeaTunnelRuntimeException(SQL_TEMPLATE_HANDLED_ERROR, params);
    }

    public static SeaTunnelRuntimeException unsupportedArrayGenericType(
            String identifier, String dataType, String fieldName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("dataType", dataType);
        params.put("fieldName", fieldName);
        return new SeaTunnelRuntimeException(UNSUPPORTED_ARRAY_GENERIC_TYPE, params);
    }

    public static SeaTunnelRuntimeException unsupportedRowKind(
            String identifier, String tableId, String rowKind) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("tableId", tableId);
        params.put("rowKind", rowKind);
        return new SeaTunnelRuntimeException(UNSUPPORTED_ROW_KIND, params);
    }

    public static SeaTunnelRuntimeException writeRowErrorWithSchemaIncompatibleSchema(
            String connector,
            String sourceFieldSqlSchema,
            String exceptFieldSqlSchema,
            String sinkFieldSqlSchema) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("sourceFieldSqlSchema", sourceFieldSqlSchema);
        params.put("exceptFieldSqlSchema", exceptFieldSqlSchema);
        params.put("sinkFieldSqlSchema", sinkFieldSqlSchema);
        return new SeaTunnelRuntimeException(
                WRITE_SEATUNNEL_ROW_ERROR_WITH_SCHEMA_INCOMPATIBLE_SCHEMA, params);
    }

    public static SeaTunnelRuntimeException writeRowErrorWithFiledsCountNotMatch(
            String connector, int sourceFieldsNum, int sinkFieldsNum) {
        Map<String, String> params = new HashMap<>();
        params.put("connector", connector);
        params.put("sourceFieldsNum", String.valueOf(sourceFieldsNum));
        params.put("sinkFieldsNum", String.valueOf(sinkFieldsNum));
        return new SeaTunnelRuntimeException(
                WRITE_SEATUNNEL_ROW_ERROR_WITH_FILEDS_NOT_MATCH, params);
    }

    public static SeaTunnelRuntimeException formatDateTimeError(String datetime, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("datetime", datetime);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CommonErrorCode.FORMAT_DATETIME_ERROR, params);
    }

    public static SeaTunnelRuntimeException formatDateError(String date, String field) {
        Map<String, String> params = new HashMap<>();
        params.put("date", date);
        params.put("field", field);
        return new SeaTunnelRuntimeException(CommonErrorCode.FORMAT_DATE_ERROR, params);
    }

    public static SeaTunnelRuntimeException unsupportedMethod(
            String identifier, String methodName) {
        Map<String, String> params = new HashMap<>();
        params.put("identifier", identifier);
        params.put("methodName", methodName);
        return new SeaTunnelRuntimeException(CommonErrorCode.UNSUPPORTED_METHOD, params);
    }
}

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
package org.apache.seatunnel.transform.jsonpath;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.COLUMNS_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.DEST_FIELD_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.PATH_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.SRC_FIELD_MUST_NOT_EMPTY;

public class JsonPathTransformConfig implements Serializable {

    public static final Option<Object> PATH =
            Options.key("path")
                    .objectType(Object.class)
                    .noDefaultValue()
                    .withDescription(
                            "JSONPath for Selecting Field from JSON. Can be a string or array of strings.");

    public static final Option<String> SRC_FIELD =
            Options.key("src_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSON source field.");

    public static final Option<Object> DEST_FIELD =
            Options.key("dest_field")
                    .objectType(Object.class)
                    .noDefaultValue()
                    .withDescription("Output field. Can be a string or array of strings.");

    public static final Option<Object> DEST_TYPE =
            Options.key("dest_type")
                    .objectType(Object.class)
                    .defaultValue("string")
                    .withDescription(
                            "Output field type. Can be a string or array of strings, default string");

    public static final Option<List<Map<String, Object>>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("columns");

    private final List<ColumnConfig> columnConfigs;
    @Getter private final ErrorHandleWay errorHandleWay;

    public List<ColumnConfig> getColumnConfigs() {
        return columnConfigs;
    }

    public JsonPathTransformConfig(
            List<ColumnConfig> columnConfigs, ErrorHandleWay errorHandleWay) {
        this.columnConfigs = columnConfigs;
        this.errorHandleWay = errorHandleWay;
    }

    public static JsonPathTransformConfig of(ReadonlyConfig config, CatalogTable table) {
        if (!config.toConfig().hasPath(COLUMNS.key())) {
            throw new TransformException(
                    COLUMNS_MUST_NOT_EMPTY, COLUMNS_MUST_NOT_EMPTY.getErrorMessage());
        }
        ErrorHandleWay rowErrorHandleWay =
                config.get(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION);
        List<Map<String, Object>> columns = config.get(COLUMNS);
        List<ColumnConfig> configs = new ArrayList<>(columns.size());
        for (Map<String, Object> map : columns) {
            checkColumnConfig(map);
            String srcField = (String) map.get(SRC_FIELD.key());
            ErrorHandleWay columnErrorHandleWay =
                    Optional.ofNullable(
                                    (String)
                                            map.get(
                                                    TransformCommonOptions
                                                            .COLUMN_ERROR_HANDLE_WAY_OPTION
                                                            .key()))
                            .map(ErrorHandleWay::valueOf)
                            .orElse(null);

            String[] pathArray = parseFields(map, PATH.key(), "path", null);
            String[] destFieldArray = parseFields(map, DEST_FIELD.key(), "dest_field", null);
            String[] typeArray = parseFields(map, DEST_TYPE.key(), "dest_type", "string");

            if (pathArray.length != destFieldArray.length || pathArray.length != typeArray.length) {
                throw new TransformException(
                        COLUMNS_MUST_NOT_EMPTY,
                        "Path, dest_field, and dest_type arrays must have the same length");
            }

            if (!table.getTableSchema().contains(srcField)) {
                throw TransformCommonError.cannotFindInputFieldError("JsonPath", srcField);
            }
            Column srcFieldColumn = table.getTableSchema().getColumn(srcField);

            for (int i = 0; i < pathArray.length; i++) {
                String path = pathArray[i].trim();
                String destField = destFieldArray[i].trim();
                String type = typeArray[i].trim();

                SeaTunnelDataType<?> srcFieldDataType =
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(srcField, type);

                Column destFieldColumn =
                        PhysicalColumn.of(
                                destField,
                                srcFieldDataType,
                                srcFieldColumn.getColumnLength(),
                                true,
                                null,
                                null);
                ColumnConfig columnConfig =
                        new ColumnConfig(
                                path, srcField, destField, destFieldColumn, columnErrorHandleWay);
                configs.add(columnConfig);
            }
        }
        return new JsonPathTransformConfig(configs, rowErrorHandleWay);
    }

    private static void checkColumnConfig(Map<String, Object> map) {
        Object pathObj = map.get(PATH.key());
        if (pathObj == null
                || (pathObj instanceof String && StringUtils.isBlank((String) pathObj))
                || (pathObj instanceof List && ((List<?>) pathObj).isEmpty())) {
            throw new TransformException(
                    PATH_MUST_NOT_EMPTY, PATH_MUST_NOT_EMPTY.getErrorMessage());
        }
        String srcField = (String) map.get(SRC_FIELD.key());
        if (StringUtils.isBlank(srcField)) {
            throw new TransformException(
                    SRC_FIELD_MUST_NOT_EMPTY, SRC_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
        Object destFieldObj = map.get(DEST_FIELD.key());
        if (destFieldObj == null
                || (destFieldObj instanceof String && StringUtils.isBlank((String) destFieldObj))
                || (destFieldObj instanceof List && ((List<?>) destFieldObj).isEmpty())) {
            throw new TransformException(
                    DEST_FIELD_MUST_NOT_EMPTY, DEST_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
    }

    /** Parse field array from configuration map */
    @SuppressWarnings("unchecked")
    private static String[] parseFields(
            Map<String, Object> map, String key, String fieldName, String defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new TransformException(
                        COLUMNS_MUST_NOT_EMPTY, String.format("%s must not be empty", fieldName));
            }
            return new String[] {defaultValue};
        }

        if (value instanceof List) {
            // Array format: ["$.data.c_string", "$.data.c_boolean"] or ["string", "boolean"]
            List<String> list = (List<String>) value;
            return list.toArray(new String[0]);
        } else if (value instanceof String) {
            // Single string value, convert to array
            return new String[] {(String) value};
        } else {
            throw new TransformException(
                    COLUMNS_MUST_NOT_EMPTY,
                    String.format("%s must be either a string or an array", fieldName));
        }
    }
}

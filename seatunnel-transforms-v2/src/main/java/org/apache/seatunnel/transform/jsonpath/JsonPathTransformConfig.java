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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.common.CommonOptions;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.exception.TransformException;

import org.apache.commons.lang3.StringUtils;

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

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSONPath for Selecting Field from JSON.");

    public static final Option<String> SRC_FIELD =
            Options.key("src_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JSON source field.");

    public static final Option<String> DEST_FIELD =
            Options.key("dest_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("output field.");

    public static final Option<String> DEST_TYPE =
            Options.key("dest_type")
                    .stringType()
                    .defaultValue("string")
                    .withDescription("output field type,default string");

    public static final Option<List<Map<String, String>>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<Map<String, String>>>() {})
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

    public static JsonPathTransformConfig of(ReadonlyConfig config) {
        if (!config.toConfig().hasPath(COLUMNS.key())) {
            throw new TransformException(
                    COLUMNS_MUST_NOT_EMPTY, COLUMNS_MUST_NOT_EMPTY.getErrorMessage());
        }
        ErrorHandleWay rowErrorHandleWay = config.get(CommonOptions.ROW_ERROR_HANDLE_WAY_OPTION);
        List<Map<String, String>> columns = config.get(COLUMNS);
        List<ColumnConfig> configs = new ArrayList<>(columns.size());
        for (Map<String, String> map : columns) {
            checkColumnConfig(map);
            String path = map.get(PATH.key());
            String srcField = map.get(SRC_FIELD.key());
            String destField = map.get(DEST_FIELD.key());
            String type = map.getOrDefault(DEST_TYPE.key(), DEST_TYPE.defaultValue());
            ErrorHandleWay columnErrorHandleWay =
                    Optional.ofNullable(map.get(CommonOptions.COLUMN_ERROR_HANDLE_WAY_OPTION.key()))
                            .map(ErrorHandleWay::valueOf)
                            .orElse(null);

            SeaTunnelDataType<?> dataType =
                    SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(srcField, type);
            ColumnConfig columnConfig =
                    new ColumnConfig(path, srcField, destField, dataType, columnErrorHandleWay);
            configs.add(columnConfig);
        }
        return new JsonPathTransformConfig(configs, rowErrorHandleWay);
    }

    private static void checkColumnConfig(Map<String, String> map) {
        String path = map.get(PATH.key());
        if (StringUtils.isBlank(path)) {
            throw new TransformException(
                    PATH_MUST_NOT_EMPTY, PATH_MUST_NOT_EMPTY.getErrorMessage());
        }
        String srcField = map.get(SRC_FIELD.key());
        if (StringUtils.isBlank(srcField)) {
            throw new TransformException(
                    SRC_FIELD_MUST_NOT_EMPTY, SRC_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
        String destField = map.get(DEST_FIELD.key());
        if (StringUtils.isBlank(destField)) {
            throw new TransformException(
                    DEST_FIELD_MUST_NOT_EMPTY, DEST_FIELD_MUST_NOT_EMPTY.getErrorMessage());
        }
    }
}

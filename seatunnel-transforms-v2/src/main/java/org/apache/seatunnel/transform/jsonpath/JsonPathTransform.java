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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.ErrorDataTransformException;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import lombok.extern.slf4j.Slf4j;

import java.time.DateTimeException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_COMPILE_ERROR;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_CONVERSION_ERROR;

@Slf4j
public class JsonPathTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "JsonPath";
    private static final Map<String, JsonPath> JSON_PATH_CACHE = new ConcurrentHashMap<>();
    private final JsonPathTransformConfig config;
    private final SeaTunnelRowType seaTunnelRowType;

    private JsonToRowConverters.JsonToObjectConverter[] converters;
    private Column[] outputColumns;

    private int[] srcFieldIndexArr;

    public JsonPathTransform(JsonPathTransformConfig config, CatalogTable catalogTable) {
        super(catalogTable, config.getErrorHandleWay());
        this.config = config;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        init();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void init() {

        initSrcFieldIndexArr();
        initOutputSeaTunnelRowType();
        initConverters();
    }

    private void initConverters() {
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        this.converters =
                this.config.getColumnConfigs().stream()
                        .map(ColumnConfig::getDestType)
                        .map(jsonToRowConverters::createConverter)
                        .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);
    }

    private void initOutputSeaTunnelRowType() {
        this.outputColumns =
                this.config.getColumnConfigs().stream()
                        .map(ColumnConfig::getDestColumn)
                        .toArray(Column[]::new);
    }

    private void initSrcFieldIndexArr() {
        List<ColumnConfig> columnConfigs = this.config.getColumnConfigs();
        Set<String> fieldNameSet = new HashSet<>(Arrays.asList(seaTunnelRowType.getFieldNames()));
        this.srcFieldIndexArr = new int[columnConfigs.size()];

        for (int i = 0; i < columnConfigs.size(); i++) {
            ColumnConfig columnConfig = columnConfigs.get(i);
            String srcField = columnConfig.getSrcField();
            if (!fieldNameSet.contains(srcField)) {
                throw TransformCommonError.cannotFindInputFieldError(getPluginName(), srcField);
            }
            this.srcFieldIndexArr[i] = seaTunnelRowType.indexOf(srcField);
        }
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        List<ColumnConfig> configs = this.config.getColumnConfigs();
        int size = configs.size();
        Object[] fieldValues = new Object[size];
        for (int i = 0; i < size; i++) {
            int pos = this.srcFieldIndexArr[i];
            ColumnConfig fieldConfig = configs.get(i);
            fieldValues[i] =
                    doTransform(
                            seaTunnelRowType.getFieldType(pos),
                            inputRow.getField(pos),
                            fieldConfig,
                            converters[i]);
        }
        return fieldValues;
    }

    private Object doTransform(
            SeaTunnelDataType<?> inputDataType,
            Object value,
            ColumnConfig columnConfig,
            JsonToRowConverters.JsonToObjectConverter converter) {
        if (value == null) {
            return null;
        }
        JSON_PATH_CACHE.computeIfAbsent(columnConfig.getPath(), JsonPath::compile);
        String jsonString = "";
        JsonNode jsonNode;
        try {
            switch (inputDataType.getSqlType()) {
                case STRING:
                    jsonString = value.toString();
                    break;
                case BYTES:
                    jsonString = new String((byte[]) value);
                    break;
                case ARRAY:
                case MAP:
                    jsonString = JsonUtils.toJsonString(value);
                    break;
                case ROW:
                    SeaTunnelRow row = (SeaTunnelRow) value;
                    jsonString = JsonUtils.toJsonString(row.getFields());
                    break;
                default:
                    throw CommonError.unsupportedDataType(
                            getPluginName(),
                            inputDataType.getSqlType().toString(),
                            columnConfig.getSrcField());
            }
            Object result = JSON_PATH_CACHE.get(columnConfig.getPath()).read(jsonString);
            jsonNode = JsonUtils.toJsonNode(result);
        } catch (JsonPathException e) {
            return handleError(columnConfig, jsonString, JSON_PATH_COMPILE_ERROR, e);
        }
        try {
            return converter.convert(jsonNode, columnConfig.getDestField());
        } catch (RuntimeException e) {
            // Nested row converters can wrap Errors; these are not skippable data failures.
            List<Throwable> causes = ExceptionUtils.getThrowableList(e);
            for (Throwable cause : causes) {
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
            }
            if (!isDataConversionFailure(causes)) {
                throw e;
            }
            return handleConversionError(columnConfig);
        }
    }

    /**
     * Accepts only numeric, date/time and wrapped JSON data failures. Every cause must be
     * recognized; unknown failures, unsupported conversions and cyclic chains fail closed.
     */
    private static boolean isDataConversionFailure(List<Throwable> causes) {
        boolean jsonOperation = false;
        for (Throwable cause : causes) {
            if (cause instanceof SeaTunnelRuntimeException) {
                SeaTunnelErrorCode code =
                        ((SeaTunnelRuntimeException) cause).getSeaTunnelErrorCode();
                if (code == CommonErrorCode.JSON_OPERATION_FAILED && cause.getCause() != null) {
                    // This wrapper also carries programming failures; inspect the rest of the
                    // chain.
                    jsonOperation = true;
                    continue;
                }
                if (code == CommonErrorCode.FORMAT_DATE_ERROR
                        || code == CommonErrorCode.FORMAT_DATETIME_ERROR) {
                    continue;
                }
            }
            if (cause instanceof NumberFormatException
                    || cause instanceof DateTimeException
                    || (jsonOperation && cause instanceof JsonProcessingException)) {
                continue;
            }
            return false;
        }
        // A cyclic cause chain has no recognized terminal data failure.
        return causes.get(causes.size() - 1).getCause() == null;
    }

    /**
     * Applies column SKIP locally and delegates other policies to the row handler. Diagnostics
     * identify a generic conversion failure, never values, paths or original exceptions.
     */
    private Object handleConversionError(ColumnConfig columnConfig) {
        if (columnConfig.errorHandleWay() != null && columnConfig.errorHandleWay().allowSkip()) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping column: {}", conversionFailureMessage(columnConfig));
            }
            return null;
        }
        throw new ErrorDataTransformException(
                columnConfig.errorHandleWay(),
                JSON_PATH_CONVERSION_ERROR,
                conversionFailureMessage(columnConfig));
    }

    private static String conversionFailureMessage(ColumnConfig columnConfig) {
        return String.format(
                "JsonPath data conversion failure, src_field=%s, dest_field=%s, dest_type=%s",
                StringUtils.abbreviate(columnConfig.getSrcField(), 128),
                StringUtils.abbreviate(columnConfig.getDestField(), 128),
                columnConfig.getDestType().getSqlType());
    }

    /**
     * Applies an explicit column policy first. Otherwise the exception leaves policy resolution to
     * the row-level handler in AbstractSeaTunnelTransform.
     */
    private Object handleError(
            ColumnConfig columnConfig,
            String jsonString,
            SeaTunnelErrorCode errorCode,
            RuntimeException cause) {
        if (columnConfig.errorHandleWay() != null && columnConfig.errorHandleWay().allowSkip()) {
            log.debug(
                    "JsonPath transform error, ignore error, config: {}, value: {}",
                    columnConfig,
                    jsonString,
                    cause);
            return null;
        }
        ErrorDataTransformException error =
                new ErrorDataTransformException(
                        columnConfig.errorHandleWay(),
                        errorCode,
                        String.format(
                                "JsonPath transform error, config: %s, value: %s, error: %s",
                                columnConfig, jsonString, cause.getMessage()));
        error.initCause(cause);
        throw error;
    }

    @Override
    protected Column[] getOutputColumns() {
        return outputColumns;
    }
}

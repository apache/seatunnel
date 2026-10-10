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
import java.util.regex.Pattern;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_COMPILE_ERROR;
import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_CONVERSION_ERROR;

@Slf4j
public class JsonPathTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "JsonPath";
    private static final Pattern SAFE_FIELD_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");
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
                            i,
                            fieldConfig,
                            converters[i]);
        }
        return fieldValues;
    }

    private Object doTransform(
            SeaTunnelDataType<?> inputDataType,
            Object value,
            int columnIndex,
            ColumnConfig columnConfig,
            JsonToRowConverters.JsonToObjectConverter converter) {
        if (value == null) {
            return null;
        }
        try {
            JSON_PATH_CACHE.computeIfAbsent(columnConfig.getPath(), JsonPath::compile);
        } catch (JsonPathException e) {
            // Invalid configuration is task-failing, regardless of the data error policy.
            throw new JsonPathException(pathFailureMessage(columnIndex, columnConfig, e));
        }
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
            return handleError(columnIndex, columnConfig, JSON_PATH_COMPILE_ERROR, e);
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
            return handleConversionError(columnIndex, columnConfig);
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
    private Object handleConversionError(int columnIndex, ColumnConfig columnConfig) {
        if (columnConfig.errorHandleWay() != null && columnConfig.errorHandleWay().allowSkip()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Skipping column: {}", conversionFailureMessage(columnIndex, columnConfig));
            }
            return null;
        }
        throw new ErrorDataTransformException(
                columnConfig.errorHandleWay(),
                JSON_PATH_CONVERSION_ERROR,
                conversionFailureMessage(columnIndex, columnConfig));
    }

    private static String conversionFailureMessage(int columnIndex, ColumnConfig columnConfig) {
        return String.format(
                "JsonPath data conversion failure, column_index=%d, src_field=%s, dest_field=%s, dest_type=%s",
                columnIndex,
                safeFieldIdentifier(columnConfig.getSrcField()),
                safeFieldIdentifier(columnConfig.getDestField()),
                columnConfig.getDestType().getSqlType());
    }

    private static String safeFieldIdentifier(String field) {
        return field != null && SAFE_FIELD_IDENTIFIER.matcher(field).matches()
                ? field
                : "<redacted>";
    }

    /**
     * Applies an explicit column policy first. Otherwise the exception leaves policy resolution to
     * the row-level handler in AbstractSeaTunnelTransform.
     */
    private Object handleError(
            int columnIndex,
            ColumnConfig columnConfig,
            SeaTunnelErrorCode errorCode,
            RuntimeException cause) {
        if (columnConfig.errorHandleWay() != null && columnConfig.errorHandleWay().allowSkip()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "JsonPath transform error, ignore error, {}",
                        pathFailureMessage(columnIndex, columnConfig, cause));
            }
            return null;
        }
        ErrorDataTransformException error =
                new ErrorDataTransformException(
                        columnConfig.errorHandleWay(),
                        errorCode,
                        pathFailureMessage(columnIndex, columnConfig, cause));
        // The original JsonPathException may contain the source or configured path in its message.
        error.initCause(new JsonPathException("Original path-reading details omitted"));
        throw error;
    }

    private static String pathFailureMessage(
            int columnIndex, ColumnConfig columnConfig, RuntimeException cause) {
        return String.format(
                "JsonPath path-reading failure, column_index=%d, src_field=%s, dest_field=%s, cause_type=%s",
                columnIndex,
                safeFieldIdentifier(columnConfig.getSrcField()),
                safeFieldIdentifier(columnConfig.getDestField()),
                cause.getClass().getSimpleName());
    }

    @Override
    protected Column[] getOutputColumns() {
        return outputColumns;
    }
}

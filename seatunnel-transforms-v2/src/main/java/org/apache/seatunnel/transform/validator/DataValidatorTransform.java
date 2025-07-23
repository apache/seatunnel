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

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.validator.ValidationResultHandler.ValidationProcessResult;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** DataValidator Transform for validating field values according to configured rules. */
@Slf4j
public class DataValidatorTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "DataValidator";
    public static final String SOURCE_TABLE_ID = "source_table_id";
    public static final String SOURCE_TABLE_PATH = "source_table_path";
    public static final String ORIGINAL_DATA = "original_data";
    public static final String VALIDATION_ERRORS = "validation_errors";
    public static final String CREATE_TIME = "create_time";

    private final DataValidatorTransformConfig config;
    private final List<FieldValidator> fieldValidators;
    private final ValidationResultHandler resultHandler;
    private final ErrorHandleWay errorHandleWay;
    private final String errorTable;

    public DataValidatorTransform(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = DataValidatorTransformConfig.of(readonlyConfig);
        this.errorHandleWay =
                readonlyConfig
                        .getOptional(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION)
                        .orElse(ErrorHandleWay.FAIL);
        // For ROUTE_TO_TABLE mode, use row_error_handle_way.error_table, otherwise fallback to
        // error_table
        this.errorTable =
                readonlyConfig
                        .getOptional(TransformCommonOptions.ERROR_TABLE_OPTION)
                        .orElse(
                                readonlyConfig
                                        .getOptional(TransformCommonOptions.ERROR_TABLE_OPTION)
                                        .orElse(null));
        this.resultHandler = new ValidationResultHandler();
        this.fieldValidators = initializeFieldValidators();
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // Execute validation for all fields
        Map<String, List<ValidationResult>> fieldResults = new HashMap<>();
        ValidationContext context =
                new ValidationContext(
                        inputRow,
                        inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                        new HashMap<>(),
                        null);

        // Always validate all fields (no fail fast)
        for (FieldValidator validator : fieldValidators) {
            String fieldName = validator.getFieldName();
            Object fieldValue = inputRow.getField(validator.getFieldIndex());

            // Update context with current field name
            ValidationContext fieldContext =
                    new ValidationContext(
                            inputRow,
                            inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                            context.getGlobalContext(),
                            fieldName);

            List<ValidationResult> results = validator.validate(fieldValue, fieldContext, false);
            fieldResults.put(fieldName, results);
        }

        // Process validation results
        ValidationProcessResult processResult =
                resultHandler.processResults(inputRow, fieldResults);

        // Handle validation failures
        if (!processResult.isValid()) {
            log.error(
                    "Validation failed for row: {}",
                    String.join("; ", processResult.getErrorMessages()));

            if (errorHandleWay == ErrorHandleWay.FAIL) {
                String message =
                        "Validation failed: " + String.join("; ", processResult.getErrorMessages());
                throw TransformCommonError.validationFailed(message);
            } else if (errorHandleWay == ErrorHandleWay.SKIP) {
                return null; // Skip this row
            } else if (errorHandleWay.allowRouteToTable()) {
                // Route invalid data to error table by setting tableId
                if (errorTable != null && !errorTable.isEmpty()) {
                    String sourceTableId = inputCatalogTable.getTableId().toString();
                    String sourceTablePath = inputCatalogTable.getTablePath().toString();
                    SeaTunnelRow errorRow =
                            generateErrorRow(
                                    inputRow,
                                    inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                                    sourceTableId,
                                    sourceTablePath,
                                    fieldResults,
                                    errorTable);
                    errorRow.setTableId(errorTable);
                    log.debug("Routing invalid data to unified error table: {}", errorTable);
                    return errorRow;
                } else {
                    log.warn("Error table not configured, skipping invalid row");
                    return null;
                }
            }
        }
        return inputRow;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> outputTables = new ArrayList<>();

        outputTables.add(getProducedCatalogTable());
        if (errorHandleWay.allowRouteToTable() && errorTable != null && !errorTable.isEmpty()) {
            TableIdentifier errorTableId =
                    TableIdentifier.of(
                            inputCatalogTable.getTableId().getCatalogName(),
                            inputCatalogTable.getTableId().getDatabaseName(),
                            errorTable);
            CatalogTable errorCatalogTable =
                    CatalogTable.of(
                            errorTableId,
                            createErrorSchema(),
                            new HashMap<>(),
                            Collections.emptyList(),
                            "Error table for validation failures");
            outputTables.add(errorCatalogTable);
        }

        return outputTables;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private List<FieldValidator> initializeFieldValidators() {
        List<FieldValidator> validators = new ArrayList<>();
        SeaTunnelRowType rowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        for (DataValidatorTransformConfig.FieldValidationRule fieldRule : config.getFieldRules()) {
            int fieldIndex = rowType.indexOf(fieldRule.getFieldName());
            if (fieldIndex >= 0) {
                validators.add(
                        new FieldValidator(
                                fieldRule.getFieldName(),
                                fieldIndex,
                                rowType.getFieldType(fieldIndex),
                                fieldRule.getRules()));
            } else {
                log.warn(
                        "Field '{}' not found in schema, skipping validation",
                        fieldRule.getFieldName());
            }
        }

        return validators;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private SeaTunnelRow generateErrorRow(
            SeaTunnelRow originalRow,
            SeaTunnelRowType originalRowType,
            String sourceTableId,
            String sourceTablePath,
            Map<String, List<ValidationResult>> fieldResults,
            String errorTable) {

        try {
            String validationErrorsJson = generateValidationErrorsJson(fieldResults);
            String originalDataJson = generateOriginalDataJson(originalRow, originalRowType);
            SeaTunnelRow errorRow = new SeaTunnelRow(5);
            errorRow.setField(0, sourceTableId);
            errorRow.setField(1, sourceTablePath);
            errorRow.setField(2, originalDataJson);
            errorRow.setField(3, validationErrorsJson);
            errorRow.setField(4, LocalDateTime.now());
            errorRow.setTableId(errorTable);

            return errorRow;

        } catch (Exception e) {
            log.error("Failed to generate unified error row", e);
            throw new RuntimeException("Failed to generate unified error row", e);
        }
    }

    private String generateValidationErrorsJson(Map<String, List<ValidationResult>> fieldResults) {
        List<Map<String, Object>> errorsList = new ArrayList<>();

        for (Map.Entry<String, List<ValidationResult>> entry : fieldResults.entrySet()) {
            String fieldName = entry.getKey();
            List<ValidationResult> results = entry.getValue();

            for (ValidationResult result : results) {
                if (!result.isValid()) {
                    Map<String, Object> errorObj = new HashMap<>();
                    errorObj.put("field_name", fieldName);
                    errorObj.put("error_message", result.getErrorMessage());
                    errorsList.add(errorObj);
                }
            }
        }

        return JsonUtils.toJsonString(errorsList);
    }

    private String generateOriginalDataJson(
            SeaTunnelRow originalRow, SeaTunnelRowType originalRowType) {
        Map<String, Object> rowMap = new HashMap<>();

        for (int i = 0; i < originalRow.getFields().length; i++) {
            String fieldName = originalRowType.getFieldName(i);
            Object fieldValue = originalRow.getField(i);
            rowMap.put(fieldName, fieldValue);
        }

        return JsonUtils.toJsonString(rowMap);
    }

    private TableSchema createErrorSchema() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of(
                                SOURCE_TABLE_ID,
                                BasicType.STRING_TYPE,
                                (Long) null,
                                false,
                                null,
                                "Source table identifier"),
                        PhysicalColumn.of(
                                SOURCE_TABLE_PATH,
                                BasicType.STRING_TYPE,
                                (Long) null,
                                false,
                                null,
                                "Source table path"),
                        PhysicalColumn.of(
                                ORIGINAL_DATA,
                                BasicType.STRING_TYPE,
                                (Long) null,
                                false,
                                null,
                                "JSON representation of the problematic row"),
                        PhysicalColumn.of(
                                VALIDATION_ERRORS,
                                BasicType.STRING_TYPE,
                                (Long) null,
                                false,
                                null,
                                "JSON array of validation error details"),
                        PhysicalColumn.of(
                                CREATE_TIME,
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                (Long) null,
                                false,
                                null,
                                "Create time of validation error"));

        return TableSchema.builder().columns(columns).build();
    }
}

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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.validator.ValidationResultHandler.ValidationProcessResult;

import org.apache.commons.collections4.map.SingletonMap;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** DataValidator Transform for validating field values according to configured rules. */
@Slf4j
public class DataValidatorTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "DataValidator";

    private final DataValidatorTransformConfig config;
    private final List<FieldValidator> fieldValidators;
    private final ValidationResultHandler resultHandler;

    public DataValidatorTransform(DataValidatorTransformConfig config, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        this.resultHandler = new ValidationResultHandler(config);
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

            if (config.getErrorHandleWay()
                    == DataValidatorTransformConfig.ValidationErrorHandleWay.FAIL) {
                Map<String, String> params =
                        new SingletonMap<>(
                                "message",
                                "Validation failed: "
                                        + String.join("; ", processResult.getErrorMessages()));
                throw new TransformException(CommonErrorCode.VALIDATION_FAILED, params);
            } else if (config.getErrorHandleWay()
                    == DataValidatorTransformConfig.ValidationErrorHandleWay.SKIP) {
                return null; // Skip this row
            } else if (config.getErrorHandleWay()
                    == DataValidatorTransformConfig.ValidationErrorHandleWay.ROUTE_TO_TABLE) {
                // Route invalid data to error table by setting tableId
                if (config.getErrorTable() != null && !config.getErrorTable().isEmpty()) {
                    SeaTunnelRow errorRow = inputRow.copy();
                    errorRow.setTableId(config.getErrorTable());
                    log.debug("Routing invalid data to error table: {}", config.getErrorTable());
                    return errorRow;
                } else {
                    log.warn("Error table not configured, skipping invalid row");
                    return null;
                }
            }
        }

        // If validation passes, return original row or row with validation columns
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {

        // Add all original columns
        List<Column> outputColumns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::copy)
                        .collect(Collectors.toList());

        // Copy constraint keys and primary key
        List<ConstraintKey> outputConstraintKeys =
                inputCatalogTable.getTableSchema().getConstraintKeys().stream()
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = null;
        PrimaryKey primaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null) {
            copiedPrimaryKey = primaryKey.copy();
        }

        return TableSchema.builder()
                .columns(outputColumns)
                .primaryKey(copiedPrimaryKey)
                .constraintKey(outputConstraintKeys)
                .build();
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
}

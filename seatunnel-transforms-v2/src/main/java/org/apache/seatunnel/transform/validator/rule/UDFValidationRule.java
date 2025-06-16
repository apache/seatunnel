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

package org.apache.seatunnel.transform.validator.rule;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.validator.ValidationContext;
import org.apache.seatunnel.transform.validator.ValidationResult;
import org.apache.seatunnel.transform.validator.udf.DataValidatorUDF;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

/**
 * Validation rule that delegates to a user-defined function (UDF) for row-level validation. This
 * rule allows users to implement custom business logic validation that can access the entire row
 * data, not just individual field values.
 */
@Data
@NoArgsConstructor
@Slf4j
public class UDFValidationRule implements ValidationRule {

    @JsonAlias("function_name")
    private String functionName;

    @JsonAlias("custom_message")
    private String customMessage;

    private transient DataValidatorUDF udfInstance;

    public UDFValidationRule(String functionName) {
        this.functionName = functionName;
        loadUDF();
    }

    public UDFValidationRule(String functionName, String customMessage) {
        this.functionName = functionName;
        this.customMessage = customMessage;
        loadUDF();
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {

        if (udfInstance == null) {
            loadUDF();
        }

        if (udfInstance == null) {
            String errorMsg = String.format("DataValidatorUDF '%s' not found", functionName);
            log.error(errorMsg);
            return ValidationResult.failure(customMessage != null ? customMessage : errorMsg);
        }

        try {
            // For UDF validation, we validate the field value like other validation rules
            ValidationResult result = udfInstance.validate(value, dataType, context);

            // If UDF validation fails and we have a custom message, use it
            if (!result.isValid() && customMessage != null) {
                return ValidationResult.failure(customMessage);
            }

            return result;
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Error executing DataValidatorUDF '%s': %s",
                            functionName, e.getMessage());
            log.error(errorMsg, e);
            return ValidationResult.failure(customMessage != null ? customMessage : errorMsg);
        }
    }

    @Override
    public String getRuleName() {
        return "UDF";
    }

    @Override
    public String getErrorMessage() {
        return customMessage != null
                ? customMessage
                : String.format("UDF validation failed: %s", functionName);
    }

    /**
     * Load the UDF instance using ServiceLoader mechanism. This method searches for all available
     * DataValidatorUDF implementations and finds the one with matching function name.
     */
    private void loadUDF() {
        if (functionName == null || functionName.trim().isEmpty()) {
            log.warn("Function name is null or empty, cannot load UDF");
            return;
        }

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            ServiceLoader<DataValidatorUDF> serviceLoader =
                    ServiceLoader.load(DataValidatorUDF.class, classLoader);

            for (DataValidatorUDF udf : serviceLoader) {
                if (functionName.equalsIgnoreCase(udf.functionName())) {
                    this.udfInstance = udf;
                    log.info("Successfully loaded DataValidatorUDF: {}", functionName);
                    return;
                }
            }

            log.warn("DataValidatorUDF '{}' not found in classpath", functionName);
        } catch (Exception e) {
            log.error("Failed to load DataValidatorUDF '{}': {}", functionName, e.getMessage(), e);
        }
    }
}

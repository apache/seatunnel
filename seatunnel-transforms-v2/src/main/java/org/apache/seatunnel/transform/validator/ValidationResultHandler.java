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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Handler for processing validation results and generating output. */
public class ValidationResultHandler implements Serializable {
    private final DataValidatorTransformConfig config;

    public ValidationResultHandler(DataValidatorTransformConfig config) {
        this.config = config;
    }

    /**
     * Process validation results for all fields and generate final result.
     *
     * @param inputRow original input row
     * @param fieldResults validation results for each field
     * @return processed validation result
     */
    public ValidationProcessResult processResults(
            SeaTunnelRow inputRow, Map<String, List<ValidationResult>> fieldResults) {

        ValidationProcessResult result = new ValidationProcessResult();
        result.setOriginalRow(inputRow);

        // Calculate validation statistics
        int totalValidations = 0;
        int failedValidations = 0;
        List<String> errorMessages = new ArrayList<>();

        for (Map.Entry<String, List<ValidationResult>> entry : fieldResults.entrySet()) {
            String fieldName = entry.getKey();
            List<ValidationResult> results = entry.getValue();

            for (ValidationResult validationResult : results) {
                totalValidations++;
                if (!validationResult.isValid()) {
                    failedValidations++;
                    errorMessages.add(
                            String.format("%s: %s", fieldName, validationResult.getErrorMessage()));
                }
            }
        }

        // Calculate validation score
        double validationScore =
                totalValidations > 0
                        ? (double) (totalValidations - failedValidations) / totalValidations
                        : 1.0;

        result.setValidationScore(validationScore);
        result.setErrorMessages(errorMessages);
        result.setValid(failedValidations == 0);

        return result;
    }

    /** Result of validation processing. */
    @Data
    public static class ValidationProcessResult implements Serializable {
        private SeaTunnelRow originalRow;
        private boolean valid;
        private double validationScore;
        private List<String> errorMessages = new ArrayList<>();
        private Map<String, Object> additionalFields = new HashMap<>();
    }
}

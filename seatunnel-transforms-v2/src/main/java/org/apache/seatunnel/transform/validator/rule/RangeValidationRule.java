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

import lombok.Data;
import lombok.NoArgsConstructor;

/** Validation rule to check if a numeric value is within a specified range. */
@Data
@NoArgsConstructor
public class RangeValidationRule implements ValidationRule {

    @JsonAlias("min_value")
    private Comparable minValue;

    @JsonAlias("max_value")
    private Comparable maxValue;

    @JsonAlias("min_inclusive")
    private boolean minInclusive = true;

    @JsonAlias("max_inclusive")
    private boolean maxInclusive = true;

    @JsonAlias("custom_message")
    private String customMessage;

    public RangeValidationRule(Comparable minValue, Comparable maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null || !(value instanceof Comparable)) {
            return ValidationResult.success();
        }

        Comparable comparableValue = (Comparable) value;

        // Check minimum value
        if (minValue != null) {
            int minComparison = comparableValue.compareTo(minValue);
            if (minInclusive ? minComparison < 0 : minComparison <= 0) {
                return ValidationResult.failure(
                        customMessage != null
                                ? customMessage
                                : String.format("Value %s is below minimum %s", value, minValue));
            }
        }

        // Check maximum value
        if (maxValue != null) {
            int maxComparison = comparableValue.compareTo(maxValue);
            if (maxInclusive ? maxComparison > 0 : maxComparison >= 0) {
                return ValidationResult.failure(
                        customMessage != null
                                ? customMessage
                                : String.format("Value %s exceeds maximum %s", value, maxValue));
            }
        }

        return ValidationResult.success();
    }

    @Override
    public String getRuleName() {
        return "RANGE";
    }

    @Override
    public String getErrorMessage() {
        return customMessage != null ? customMessage : "Value out of range";
    }
}

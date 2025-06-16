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

import java.util.Collection;

/** Validation rule to check the length of string, array, or collection values. */
@Data
@NoArgsConstructor
public class LengthValidationRule implements ValidationRule {

    @JsonAlias("min_length")
    private Integer minLength;

    @JsonAlias("max_length")
    private Integer maxLength;

    @JsonAlias("exact_length")
    private Integer exactLength;

    @JsonAlias("custom_message")
    private String customMessage;

    public LengthValidationRule(Integer minLength, Integer maxLength) {
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public LengthValidationRule(Integer exactLength) {
        this.exactLength = exactLength;
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null) {
            return ValidationResult.success();
        }

        int length = getLength(value);

        if (exactLength != null && length != exactLength) {
            return ValidationResult.failure(
                    customMessage != null
                            ? customMessage
                            : String.format("Expected length %d but got %d", exactLength, length));
        }

        if (minLength != null && length < minLength) {
            return ValidationResult.failure(
                    customMessage != null
                            ? customMessage
                            : String.format("Length %d is below minimum %d", length, minLength));
        }

        if (maxLength != null && length > maxLength) {
            return ValidationResult.failure(
                    customMessage != null
                            ? customMessage
                            : String.format("Length %d exceeds maximum %d", length, maxLength));
        }

        return ValidationResult.success();
    }

    @Override
    public String getRuleName() {
        return "LENGTH";
    }

    @Override
    public String getErrorMessage() {
        return customMessage != null ? customMessage : "Length validation failed";
    }

    private int getLength(Object value) {
        if (value instanceof String) {
            return ((String) value).length();
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).length;
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).size();
        }
        return value.toString().length();
    }
}

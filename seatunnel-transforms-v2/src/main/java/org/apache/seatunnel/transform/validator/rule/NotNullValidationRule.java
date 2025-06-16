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

/** Validation rule to check if a field value is not null. */
@Data
@NoArgsConstructor
public class NotNullValidationRule implements ValidationRule {

    @JsonAlias("custom_message")
    private String customMessage;

    public NotNullValidationRule(String customMessage) {
        this.customMessage = customMessage;
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null) {
            return ValidationResult.failure(
                    customMessage != null ? customMessage : "Field cannot be null");
        }
        return ValidationResult.success();
    }

    @Override
    public String getRuleName() {
        return "NOT_NULL";
    }

    @Override
    public String getErrorMessage() {
        return customMessage != null ? customMessage : "Field cannot be null";
    }
}

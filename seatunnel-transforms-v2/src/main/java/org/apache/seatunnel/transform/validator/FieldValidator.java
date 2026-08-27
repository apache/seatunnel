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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.validator.rule.ValidationRule;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Validator for a specific field, containing multiple validation rules. */
@Data
public class FieldValidator implements Serializable {
    private final String fieldName;
    private final int fieldIndex;
    private final SeaTunnelDataType<?> fieldDataType;
    private final List<ValidationRule> rules;

    public FieldValidator(
            String fieldName,
            int fieldIndex,
            SeaTunnelDataType<?> fieldDataType,
            List<ValidationRule> rules) {
        this.fieldName = fieldName;
        this.fieldIndex = fieldIndex;
        this.fieldDataType = fieldDataType;
        this.rules = rules != null ? rules : new ArrayList<>();
    }

    /**
     * Validate the field value using all configured rules.
     *
     * @param fieldValue the value to validate
     * @param context validation context
     * @param failFast whether to stop on first failure
     * @return list of validation results
     */
    public List<ValidationResult> validate(
            Object fieldValue, ValidationContext context, boolean failFast) {
        List<ValidationResult> results = new ArrayList<>();

        for (ValidationRule rule : rules) {
            ValidationResult result = rule.validate(fieldValue, fieldDataType, context);
            results.add(result);

            // If fail fast mode and validation failed, stop here
            if (failFast && !result.isValid()) {
                break;
            }
        }

        return results;
    }
}

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

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.validator.ValidationContext;
import org.apache.seatunnel.transform.validator.ValidationResult;

import java.io.Serializable;

/**
 * Base interface for all validation rules. Each validation rule defines how to validate a specific
 * aspect of field data.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "rule_type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = NotNullValidationRule.class, name = "NOT_NULL"),
    @JsonSubTypes.Type(value = RangeValidationRule.class, name = "RANGE"),
    @JsonSubTypes.Type(value = LengthValidationRule.class, name = "LENGTH"),
    @JsonSubTypes.Type(value = RegexValidationRule.class, name = "REGEX"),
    @JsonSubTypes.Type(value = UDFValidationRule.class, name = "UDF")
})
public interface ValidationRule extends Serializable {

    /**
     * Validate the given value according to this rule.
     *
     * @param value the value to validate
     * @param dataType the data type of the field
     * @param context the validation context
     * @return validation result
     */
    ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context);

    /**
     * Get the name of this validation rule.
     *
     * @return rule name
     */
    String getRuleName();

    /**
     * Get the default error message for this rule.
     *
     * @return error message
     */
    String getErrorMessage();
}

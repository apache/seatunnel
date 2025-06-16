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

import java.util.regex.Pattern;

/** Validation rule to check if a string value matches a regular expression pattern. */
@Data
@NoArgsConstructor
public class RegexValidationRule implements ValidationRule {

    @JsonAlias("pattern")
    private String pattern;

    @JsonAlias("case_sensitive")
    private boolean caseSensitive = true;

    @JsonAlias("custom_message")
    private String customMessage;

    private transient Pattern compiledPattern;

    public RegexValidationRule(String pattern) {
        this.pattern = pattern;
        compilePattern();
    }

    public RegexValidationRule(String pattern, boolean caseSensitive) {
        this.pattern = pattern;
        this.caseSensitive = caseSensitive;
        compilePattern();
    }

    private void compilePattern() {
        if (pattern != null) {
            int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
            this.compiledPattern = Pattern.compile(pattern, flags);
        }
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        if (value == null) {
            return ValidationResult.success();
        }

        if (compiledPattern == null) {
            compilePattern();
        }

        String stringValue = value.toString();
        if (!compiledPattern.matcher(stringValue).matches()) {
            return ValidationResult.failure(
                    customMessage != null
                            ? customMessage
                            : String.format(
                                    "Value '%s' does not match pattern '%s'",
                                    stringValue, pattern));
        }

        return ValidationResult.success();
    }

    @Override
    public String getRuleName() {
        return "REGEX";
    }

    @Override
    public String getErrorMessage() {
        return customMessage != null ? customMessage : "Pattern validation failed";
    }
}

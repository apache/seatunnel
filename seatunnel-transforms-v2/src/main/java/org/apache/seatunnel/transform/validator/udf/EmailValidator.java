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

package org.apache.seatunnel.transform.validator.udf;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.validator.ValidationContext;
import org.apache.seatunnel.transform.validator.ValidationResult;

import com.google.auto.service.AutoService;

import java.util.regex.Pattern;

@AutoService(DataValidatorUDF.class)
public class EmailValidator implements DataValidatorUDF {

    private static final Pattern DOMAIN_PATTERN = Pattern.compile("^[a-zA-Z0-9.-]+$");

    private static final int MAX_EMAIL_LENGTH = 254;
    private static final int MAX_LOCAL_PART_LENGTH = 63;

    @Override
    public String functionName() {
        return "EMAIL";
    }

    @Override
    public ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context) {
        // Skip validation if value is null
        if (value == null) {
            return ValidationResult.success();
        }

        String email = value.toString().trim();

        // Skip validation if empty
        if (email.isEmpty()) {
            return ValidationResult.success();
        }

        // Basic length check
        if (email.length() > MAX_EMAIL_LENGTH) {
            return ValidationResult.failure(
                    "Email too long (max " + MAX_EMAIL_LENGTH + " characters): " + email);
        }

        // Must contain exactly one @ symbol
        int atIndex = email.indexOf('@');
        if (atIndex <= 0 || atIndex != email.lastIndexOf('@')) {
            return ValidationResult.failure("Email must contain exactly one @ symbol: " + email);
        }

        // Split into local and domain parts
        String localPart = email.substring(0, atIndex);
        String domainPart = email.substring(atIndex + 1);

        // Validate local part
        if (localPart.length() > MAX_LOCAL_PART_LENGTH) {
            return ValidationResult.failure(
                    "Email local part too long (max "
                            + MAX_LOCAL_PART_LENGTH
                            + " characters): "
                            + email);
        }

        // Check for dangerous characters (basic security check)
        if (email.contains("\"")
                || email.contains("'")
                || email.contains("`")
                || email.contains("\0")) {
            return ValidationResult.failure("Email contains dangerous characters: " + email);
        }

        // Validate domain part format
        if (!DOMAIN_PATTERN.matcher(domainPart).matches()) {
            return ValidationResult.failure("Email domain contains invalid characters: " + email);
        }

        // Domain must contain at least one dot
        if (!domainPart.contains(".")) {
            return ValidationResult.failure("Email domain must contain at least one dot: " + email);
        }

        return ValidationResult.success();
    }

    @Override
    public String getDescription() {
        return "Practical email validation based on OWASP recommendations";
    }
}

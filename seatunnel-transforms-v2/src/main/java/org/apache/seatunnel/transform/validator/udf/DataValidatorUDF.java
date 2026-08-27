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

import java.io.Serializable;

public interface DataValidatorUDF extends Serializable {

    /**
     * Get the unique name of this validation function. This name will be used in configuration to
     * reference this UDF.
     *
     * @return function name (should be unique across all DataValidatorUDFs)
     */
    String functionName();

    /**
     * Validate a single field value using custom business logic. This method receives a single
     * field value and can perform custom validation logic specific to that field.
     *
     * @param value the field value to validate
     * @param dataType the data type of the field
     * @param context validation context containing additional information
     * @return validation result indicating success or failure with error message
     */
    ValidationResult validate(
            Object value, SeaTunnelDataType<?> dataType, ValidationContext context);

    /**
     * Get a description of what this validation function does. This is used for documentation and
     * error reporting purposes.
     *
     * @return description of the validation function
     */
    default String getDescription() {
        return "Custom validation function: " + functionName();
    }
}

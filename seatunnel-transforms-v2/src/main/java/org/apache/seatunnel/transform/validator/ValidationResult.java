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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/** Result of a validation operation. */
@Data
@AllArgsConstructor
public class ValidationResult implements Serializable {
    private boolean valid;
    private String errorMessage;

    /**
     * Create a successful validation result.
     *
     * @return success result
     */
    public static ValidationResult success() {
        return new ValidationResult(true, null);
    }

    /**
     * Create a failed validation result.
     *
     * @param message error message
     * @return failure result
     */
    public static ValidationResult failure(String message) {
        return new ValidationResult(false, message);
    }
}

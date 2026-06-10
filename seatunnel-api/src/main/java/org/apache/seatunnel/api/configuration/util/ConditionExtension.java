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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

public interface ConditionExtension<T> {

    /**
     * Human-readable description of this validation rule. Used in error messages ({@link
     * Condition#toString()}) and REST metadata ({@code OptionRulesService}).
     *
     * @return non-null description, e.g. {@code "must be between 1 and 65535"}
     */
    String description();

    /**
     * Evaluates whether {@code value} passes this validation rule.
     *
     * <p>Return {@code false} for simple failure — the framework composes the error from {@link
     * #description()} automatically. Throw {@link OptionValidationException} when a richer message
     * is needed. Avoid other unchecked exceptions — they propagate unwrapped.
     *
     * @param config full configuration context (read-only), available for cross-field checks
     * @param value the resolved option value; may be {@code null}
     * @return {@code true} if valid
     * @throws OptionValidationException for detailed error reporting
     */
    boolean evaluate(ReadonlyConfig config, T value) throws OptionValidationException;
}

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
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.api.configuration.util.issue.DeprecatedConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.VersionCompatibilityConfigurationIssue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An enhanced configuration validator used to perform rule-based verification on connector or
 * component configurations.
 *
 * <p>This validator focuses on non-fatal configuration issues such as deprecations, conflicts, and
 * version compatibility. Validation results are returned as structured verification outputs rather
 * than throwing exceptions directly, allowing the caller to aggregate and classify errors and
 * warnings.
 */
public interface EnhancedConfigurationValidator {

    /**
     * Execute all configuration validation rules and aggregate the results.
     *
     * <p>This method serves as a unified entry point for configuration verification. It
     * sequentially executes all supported validation categories, including:
     *
     * <ul>
     *   <li>Deprecated parameter validation
     *   <li>Conflicting parameter validation
     *   <li>Version compatibility validation
     * </ul>
     *
     * <p>The method aggregates results from each rule category into a single list. All rule methods
     * must return non-null lists; defaults use {@link Collections#emptyList()} to avoid
     * boilerplate.
     *
     * @return a combined list of configuration verification results from all validation rules; an
     *     empty list if no issues are detected
     */
    default List<ConfigurationVerificationIssue> validate(ReadonlyConfig context) {
        List<ConfigurationVerificationIssue> results = new ArrayList<>();
        results.addAll(validateDeprecatedRules(context));
        results.addAll(validateConflictRules(context));
        results.addAll(validateVersionCompatibilityRules(context));
        return results;
    }

    /**
     * Validate rules related to deprecated configuration parameters.
     *
     * <p>This validation checks whether any configuration parameters have been marked as deprecated
     * and provides guidance or suggestions for recommended replacements. Defaults to no findings.
     *
     * @return a list of verification results describing deprecated parameters and suggested
     *     alternatives; empty if none found
     */
    default List<DeprecatedConfigurationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        return Collections.emptyList();
    }

    /**
     * Validate rules related to conflicting configuration parameters.
     *
     * <p>This validation detects mutually exclusive, logically conflicting, or incompatible
     * configuration options. Defaults to no findings.
     *
     * <p>Examples include parameters that cannot be enabled at the same time or options whose
     * semantics override each other.
     *
     * @return a list of verification results describing configuration conflicts; empty if no
     *     conflicts are detected
     */
    default List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {
        return Collections.emptyList();
    }

    /**
     * Validate rules related to version compatibility and constraints.
     *
     * <p>This validation ensures that configuration parameters are compatible with the runtime
     * environment, connector version, or external system version. Defaults to no findings.
     *
     * <p>Examples include parameters introduced in specific versions, options removed in later
     * versions, or features only supported by certain engine or service versions.
     *
     * @return a list of verification results describing version incompatibilities or constraints;
     *     empty if all parameters are version-compatible
     */
    default List<VersionCompatibilityConfigurationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {
        return Collections.emptyList();
    }
}

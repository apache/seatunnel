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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Rule-based configuration checker that reports issues instead of throwing immediately. */
public interface EnhancedConfigurationValidator {

    Logger LOG = LoggerFactory.getLogger(EnhancedConfigurationValidator.class);

    /** Run all rule categories and merge the results. */
    default List<ConfigurationVerificationIssue> validate(ReadonlyConfig context) {
        List<ConfigurationVerificationIssue> results = new ArrayList<>();
        results.addAll(
                runRule(
                        "deprecated configuration validation",
                        () -> validateDeprecatedRules(context),
                        Collections.emptyList()));
        results.addAll(
                runRule(
                        "conflict configuration validation",
                        () -> validateConflictRules(context),
                        Collections.emptyList()));
        results.addAll(
                runRule(
                        "version compatibility configuration validation",
                        () -> validateVersionCompatibilityRules(context),
                        Collections.emptyList()));
        return results;
    }

    /** Report deprecated options and suggested replacements. */
    List<DeprecatedConfigurationIssue> validateDeprecatedRules(ReadonlyConfig context);

    /** Report conflicts between configuration options. */
    List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context);

    /** Report options that are incompatible with the current/required version. */
    List<VersionCompatibilityConfigurationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context);

    static <T> List<T> runRule(String ruleName, Supplier<List<T>> rule, List<T> defaultOnFailure) {
        try {
            List<T> result = rule.get();
            return result == null ? defaultOnFailure : result;
        } catch (Exception e) {
            LOG.warn("Enhanced configuration {} failed, skip with default", ruleName, e);
            return defaultOnFailure;
        }
    }
}

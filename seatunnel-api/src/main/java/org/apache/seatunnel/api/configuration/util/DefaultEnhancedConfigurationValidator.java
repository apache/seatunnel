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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue.Level;
import org.apache.seatunnel.api.configuration.util.issue.ConflictConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.DeprecatedConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.VersionCompatibilityConfigurationIssue;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    protected final String identifier;
    protected final PluginType pluginType;

    @Override
    public List<DeprecatedConfigurationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        final List<Option<?>> deprecateOptions = deprecatedOptions(context);
        if (deprecateOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return deprecateOptions.stream()
                .map(option -> DeprecatedConfigurationIssue.of(identifier, pluginType, option))
                .collect(Collectors.toList());
    }

    protected abstract List<Option<?>> deprecatedOptions(ReadonlyConfig context);

    @Override
    public List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {
        List<ConflictOption> conflictOptions = conflictOptions(context);
        if (conflictOptions == null || conflictOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return conflictOptions.stream()
                .map(
                        conflict -> {
                            if (Level.ERROR.equals(conflict.level)) {
                                return ConflictConfigurationIssue.errorOf(
                                        identifier,
                                        pluginType,
                                        conflict.option,
                                        conflict.value,
                                        conflict.conflictOption);
                            }
                            return ConflictConfigurationIssue.warnOf(
                                    identifier,
                                    pluginType,
                                    conflict.option,
                                    conflict.value,
                                    conflict.conflictOption);
                        })
                .collect(Collectors.toList());
    }

    protected abstract List<ConflictOption> conflictOptions(ReadonlyConfig context);

    @Override
    public List<VersionCompatibilityConfigurationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {
        List<VersionCompatibilityOption> compatibilityOptions =
                versionCompatibilityOptions(context);
        if (compatibilityOptions == null || compatibilityOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return compatibilityOptions.stream()
                .map(
                        option -> {
                            Optional<String> currentVersion = detectCurrentServiceVersion(context);
                            if (Level.ERROR.equals(option.level)) {
                                return VersionCompatibilityConfigurationIssue.errorOf(
                                        identifier,
                                        pluginType,
                                        option.option,
                                        option.needVersion,
                                        currentVersion);
                            }
                            return VersionCompatibilityConfigurationIssue.warnOf(
                                    identifier,
                                    pluginType,
                                    option.option,
                                    option.needVersion,
                                    currentVersion);
                        })
                .collect(Collectors.toList());
    }

    protected abstract List<VersionCompatibilityOption> versionCompatibilityOptions(
            ReadonlyConfig context);

    /**
     * Detect the version of the external service used by the connector.
     *
     * <p>Implementations may return {@link Optional#empty()} if detection is not supported.
     */
    protected abstract Optional<String> detectCurrentServiceVersion(ReadonlyConfig context);

    protected static class ConflictOption {
        private final Level level;
        private final Option<?> option;
        private final Object value;
        private final Option<?> conflictOption;

        public ConflictOption(
                Level level, Option<?> option, Object value, Option<?> conflictOption) {
            this.level = level;
            this.option = option;
            this.value = value;
            this.conflictOption = conflictOption;
        }
    }

    protected static class VersionCompatibilityOption {
        private final Level level;
        private final Option<?> option;
        private final String needVersion;

        public VersionCompatibilityOption(
                Level level,
                Option<?> option,
                String needVersion,
                Optional<String> currentVersion) {
            this.level = level;
            this.option = option;
            this.needVersion = needVersion;
        }
    }
}

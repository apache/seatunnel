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
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    protected final String identifier;
    protected final PluginType pluginType;

    @Override
    public List<DeprecatedConfigurationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        final List<DeprecatedOption> deprecateOptions = deprecatedOptions(context);
        if (deprecateOptions == null || deprecateOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return deprecateOptions.stream()
                .map(
                        deprecatedOption ->
                                DeprecatedConfigurationIssue.of(
                                        identifier,
                                        pluginType,
                                        deprecatedOption.option,
                                        deprecatedOption.referToOption))
                .collect(Collectors.toList());
    }

    protected abstract List<DeprecatedOption> deprecatedOptions(ReadonlyConfig context);

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
        Optional<String> currentVersion = detectCurrentServiceVersion(context);
        return compatibilityOptions.stream()
                .map(
                        option -> {
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

    protected Optional<String> detectCurrentServiceVersion(ReadonlyConfig context) {
        try {
            Optional<Catalog> catalogOptional = getCatalog(context);
            if (!catalogOptional.isPresent()) {
                return Optional.empty();
            }
            try (Catalog catalog = catalogOptional.get()) {
                catalog.open();
                return catalog.getServiceVersion();
            }
        } catch (Exception e) {
            log.warn("Failed to detect service version via catalog", e);
            return Optional.empty();
        }
    }

    protected abstract Optional<Catalog> getCatalog(ReadonlyConfig context);

    /** Defines a deprecated option and its suggested replacements. */
    protected static class DeprecatedOption {
        private final Option<?> option;
        private final Option<?>[] referToOption;

        private DeprecatedOption(Option<?> option, Option<?>[] referToOptions) {
            this.option = option;
            this.referToOption = referToOptions;
        }

        public static DeprecatedOption warning(Option<?> option, Option<?>[] referToOptions) {
            return new DeprecatedOption(option, referToOptions);
        }

        public static DeprecatedOption warning(Option<?> option) {
            return new DeprecatedOption(option, null);
        }
    }

    /** Represents a conflicting option pair and severity. */
    protected static class ConflictOption {
        private final Level level;
        private final Option<?> option;
        private final Object value;
        private final Option<?> conflictOption;

        private ConflictOption(
                Level level, Option<?> option, Object value, Option<?> conflictOption) {
            this.level = level;
            this.option = option;
            this.value = value;
            this.conflictOption = conflictOption;
        }

        public static ConflictOption warning(
                Option<?> option, Object value, Option<?> conflictOption) {
            return new ConflictOption(Level.WARNING, option, value, conflictOption);
        }

        public static ConflictOption error(
                Option<?> option, Object value, Option<?> conflictOption) {
            return new ConflictOption(Level.ERROR, option, value, conflictOption);
        }
    }

    /** Captures version requirements for an option and severity. */
    protected static class VersionCompatibilityOption {
        private final Level level;
        private final Option<?> option;
        private final String needVersion;

        private VersionCompatibilityOption(Level level, Option<?> option, String needVersion) {
            this.level = level;
            this.option = option;
            this.needVersion = needVersion;
        }

        public static VersionCompatibilityOption warning(Option<?> option, String needVersion) {
            return new VersionCompatibilityOption(Level.WARNING, option, needVersion);
        }

        public static VersionCompatibilityOption error(Option<?> option, String needVersion) {
            return new VersionCompatibilityOption(Level.ERROR, option, needVersion);
        }
    }
}

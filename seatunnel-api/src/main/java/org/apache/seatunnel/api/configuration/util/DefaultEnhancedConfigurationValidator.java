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
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    protected final String identifier;
    protected final PluginType pluginType;

    @Override
    public List<DeprecatedConfigurationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        final List<DeprecatedRule> deprecateOptions = deprecatedRules();
        if (deprecateOptions == null || deprecateOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return deprecateOptions.stream()
                .filter(option -> context.getOptional(option.option).isPresent())
                .map(
                        deprecatedOption ->
                                DeprecatedConfigurationIssue.of(
                                        identifier,
                                        pluginType,
                                        deprecatedOption.option,
                                        deprecatedOption.referToOption))
                .collect(Collectors.toList());
    }

    protected abstract List<DeprecatedRule> deprecatedRules();

    @Override
    public List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {
        List<ConflictRule> conflictOptions = conflictRules();
        if (conflictOptions == null || conflictOptions.isEmpty()) {
            return Collections.emptyList();
        }
        return conflictOptions.stream()
                .map(conflict -> validateConflict(context, conflict))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<ConfigurationVerificationIssue> validateConflict(
            ReadonlyConfig context, ConflictRule conflict) {
        Optional<?> optionValue = context.getOptional(conflict.option);
        Optional<?> conflictOptionValue = context.getOptional(conflict.conflictOption);
        if (!optionValue.isPresent() || !conflictOptionValue.isPresent()) {
            return Optional.empty();
        }
        if (!conflict.conflictingValidationRules.test(
                optionValue.get(), conflictOptionValue.get())) {
            return Optional.empty();
        }
        if (Level.ERROR.equals(conflict.level)) {
            return Optional.of(
                    ConflictConfigurationIssue.errorOf(
                            identifier,
                            pluginType,
                            conflict.option,
                            optionValue.get(),
                            conflict.conflictOption));
        }
        return Optional.of(
                ConflictConfigurationIssue.warnOf(
                        identifier,
                        pluginType,
                        conflict.option,
                        optionValue.get(),
                        conflict.conflictOption));
    }

    protected abstract List<ConflictRule> conflictRules();

    @Override
    public List<VersionCompatibilityConfigurationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {
        List<VersionCompatibilityRule> compatibilityOptions = versionCompatibilityRules();
        if (compatibilityOptions == null || compatibilityOptions.isEmpty()) {
            return Collections.emptyList();
        }
        Optional<String> currentVersion = detectCurrentServiceVersion(context);
        return compatibilityOptions.stream()
                .map(option -> validateCompatibility(context, currentVersion, option))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<VersionCompatibilityConfigurationIssue> validateCompatibility(
            ReadonlyConfig context,
            Optional<String> currentVersion,
            VersionCompatibilityRule option) {
        if (!context.getOptional(option.option).isPresent()) {
            return Optional.empty();
        }
        if (option.isCompatible(currentVersion)) {
            return Optional.empty();
        }
        if (Level.ERROR.equals(option.level)) {
            return Optional.of(
                    VersionCompatibilityConfigurationIssue.errorOf(
                            identifier,
                            pluginType,
                            option.option,
                            option.needVersion,
                            currentVersion));
        }
        return Optional.of(
                VersionCompatibilityConfigurationIssue.warnOf(
                        identifier, pluginType, option.option, option.needVersion, currentVersion));
    }

    protected abstract List<VersionCompatibilityRule> versionCompatibilityRules();

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

    protected Optional<Catalog> getCatalog(ReadonlyConfig context) {
        return Optional.empty();
    }

    /** Rule describing a deprecated option and its suggested replacements. */
    protected static class DeprecatedRule {
        private final Option<?> option;
        private final Option<?>[] referToOption;

        private DeprecatedRule(Option<?> option, Option<?>[] referToOptions) {
            this.option = option;
            this.referToOption = referToOptions;
        }

        public static DeprecatedRule warning(Option<?> option, Option<?>[] referToOptions) {
            return new DeprecatedRule(option, referToOptions);
        }

        public static DeprecatedRule warning(Option<?> option) {
            return new DeprecatedRule(option, null);
        }
    }

    /** Rule describing conflicting option pairs and severity. */
    protected static class ConflictRule {
        private final Level level;
        private final Option<?> option;
        private final Option<?> conflictOption;
        private final BiPredicate<Object, Object> conflictingValidationRules;

        private ConflictRule(
                Level level,
                Option<?> option,
                BiPredicate<Object, Object> conflictingValidationRules,
                Option<?> conflictOption) {
            this.level = level;
            this.option = option;
            this.conflictingValidationRules =
                    conflictingValidationRules == null
                            ? (value, conflictValue) -> true
                            : conflictingValidationRules;
            this.conflictOption = conflictOption;
        }

        public static ConflictRule warning(Option<?> option, Option<?> conflictOption) {
            return new ConflictRule(Level.WARNING, option, null, conflictOption);
        }

        public static ConflictRule warning(
                Option<?> option, BiPredicate<Object, Object> rules, Option<?> conflictOption) {
            return new ConflictRule(Level.WARNING, option, rules, conflictOption);
        }

        public static ConflictRule error(Option<?> option, Option<?> conflictOption) {
            return new ConflictRule(Level.ERROR, option, null, conflictOption);
        }

        public static ConflictRule error(
                Option<?> option, BiPredicate<Object, Object> rules, Option<?> conflictOption) {
            return new ConflictRule(Level.ERROR, option, rules, conflictOption);
        }
    }

    /** Rule capturing version requirements for an option and severity. */
    protected static class VersionCompatibilityRule {
        private final Level level;
        private final Option<?> option;
        private final String needVersion;
        private final Predicate<String> compatibilityValidationRules;

        private VersionCompatibilityRule(
                Level level,
                Option<?> option,
                String needVersion,
                Predicate<String> compatibilityValidationRules) {
            this.level = level;
            this.option = option;
            this.needVersion = needVersion;
            this.compatibilityValidationRules =
                    compatibilityValidationRules == null
                            ? version -> version.equals(needVersion)
                            : compatibilityValidationRules;
        }

        public static VersionCompatibilityRule warning(
                Option<?> option,
                Predicate<String> compatibilityValidationRules,
                String needVersion) {
            return new VersionCompatibilityRule(
                    Level.WARNING, option, needVersion, compatibilityValidationRules);
        }

        public static VersionCompatibilityRule error(
                Option<?> option,
                Predicate<String> compatibilityValidationRules,
                String needVersion) {
            return new VersionCompatibilityRule(
                    Level.ERROR, option, needVersion, compatibilityValidationRules);
        }

        private boolean isCompatible(Optional<String> currentVersion) {
            return currentVersion.map(compatibilityValidationRules::test).orElse(false);
        }
    }
}

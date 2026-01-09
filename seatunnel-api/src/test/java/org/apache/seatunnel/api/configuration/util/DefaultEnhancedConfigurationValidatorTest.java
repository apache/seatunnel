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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue.Level;
import org.apache.seatunnel.api.configuration.util.issue.ConflictConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.DeprecatedConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.VersionCompatibilityConfigurationIssue;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class DefaultEnhancedConfigurationValidatorTest {

    private static final Option<String> LEGACY_OPTION =
            Options.key("legacy.option")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("deprecated option for testing");

    private static final Option<String> REPLACEMENT_OPTION =
            Options.key("replacement.option")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("replacement option for testing");

    private static final Option<String> PRIMARY_MODE_OPTION =
            new Option<>("primary.mode", new TypeReference<String>() {}, "default");

    private static final Option<String> SECONDARY_MODE_OPTION =
            new Option<>("secondary.mode", new TypeReference<String>() {}, "default");

    private static final Option<String> VERSION_GATED_OPTION =
            Options.key("version.gated.option")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("option requires compatible service version");

    private static final Option<String> RELAXED_VERSION_OPTION =
            Options.key("relaxed.version.option")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("option compatible with current catalog version");

    private final TestValidator validator = new TestValidator();

    @Test
    void shouldCollectAllIssuesWithExpectedDetails() {
        Map<String, Object> config = new HashMap<>();
        config.put(LEGACY_OPTION.key(), "legacy-value");
        config.put(PRIMARY_MODE_OPTION.key(), "duplicate");
        config.put(SECONDARY_MODE_OPTION.key(), "duplicate");
        config.put(VERSION_GATED_OPTION.key(), "enabled");

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(3, issues.size());

        DeprecatedConfigurationIssue deprecatedIssue =
                findIssue(issues, DeprecatedConfigurationIssue.class);
        Assertions.assertEquals(Level.WARNING, deprecatedIssue.getLevel());
        Assertions.assertEquals("demo-id", deprecatedIssue.getIdentifier());
        Assertions.assertEquals(PluginType.SOURCE, deprecatedIssue.getPluginType());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Deprecated configuration option 'legacy.option' detected in source plugin 'demo-id', please refer to replacement.option",
                deprecatedIssue.getLog());

        ConflictConfigurationIssue conflictIssue =
                findIssue(issues, ConflictConfigurationIssue.class);
        Assertions.assertEquals(Level.ERROR, conflictIssue.getLevel());
        Assertions.assertEquals("demo-id", conflictIssue.getIdentifier());
        Assertions.assertEquals(PluginType.SOURCE, conflictIssue.getPluginType());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'primary.mode' with value 'duplicate' conflicts with option 'secondary.mode' (value 'duplicate') in source plugin 'demo-id'",
                conflictIssue.getLog());

        VersionCompatibilityConfigurationIssue compatibilityIssue =
                findIssue(issues, VersionCompatibilityConfigurationIssue.class);
        Assertions.assertEquals(Level.ERROR, compatibilityIssue.getLevel());
        Assertions.assertEquals("demo-id", compatibilityIssue.getIdentifier());
        Assertions.assertEquals(PluginType.SOURCE, compatibilityIssue.getPluginType());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'version.gated.option' requires version '2.x' (current version '1.0.0') in source plugin 'demo-id'",
                compatibilityIssue.getLog());
    }

    @Test
    void shouldSkipIssuesWhenPredicatesDoNotMatch() {
        Map<String, Object> config = new HashMap<>();
        config.put(PRIMARY_MODE_OPTION.key(), "keep");
        config.put(SECONDARY_MODE_OPTION.key(), "merge");
        config.put(RELAXED_VERSION_OPTION.key(), "enabled");

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertTrue(issues.isEmpty());
    }

    private <T extends ConfigurationVerificationIssue> T findIssue(
            List<ConfigurationVerificationIssue> issues, Class<T> issueClass) {
        return issueClass.cast(
                issues.stream()
                        .filter(issueClass::isInstance)
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected issue of type " + issueClass.getName())));
    }

    private static class TestValidator extends DefaultEnhancedConfigurationValidator {

        private TestValidator() {
            super("demo-id", PluginType.SOURCE, Optional.of(new FakeCatalog()));
        }

        @Override
        protected List<DeprecatedRule> deprecatedRules() {
            return Collections.singletonList(
                    DeprecatedRule.warning(LEGACY_OPTION, new Option[] {REPLACEMENT_OPTION}));
        }

        @Override
        protected List<ConflictRule> conflictRules() {
            BiPredicate<Object, Object> duplicatedValue =
                    (first, second) ->
                            "duplicate".equalsIgnoreCase(String.valueOf(first))
                                    && "duplicate".equalsIgnoreCase(String.valueOf(second));
            return Collections.singletonList(
                    ConflictRule.error(
                            PRIMARY_MODE_OPTION, duplicatedValue, SECONDARY_MODE_OPTION));
        }

        @Override
        protected List<VersionCompatibilityRule> versionCompatibilityRules() {
            List<VersionCompatibilityRule> versionCompatibilityRules = new ArrayList<>();
            Predicate<String> versionStartsWithTwo = version -> version.startsWith("2.");
            Predicate<String> versionStartsWithOne = version -> version.startsWith("1.");
            versionCompatibilityRules.add(
                    VersionCompatibilityRule.error(
                            VERSION_GATED_OPTION, versionStartsWithTwo, "2.x"));
            versionCompatibilityRules.add(
                    VersionCompatibilityRule.warning(
                            RELAXED_VERSION_OPTION, versionStartsWithOne, "1.x"));
            return versionCompatibilityRules;
        }
    }

    private static class FakeCatalog implements Catalog {

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public String name() {
            return "fake-catalog";
        }

        @Override
        public Optional<String> getServiceVersion() {
            return Optional.of("1.0.0");
        }

        @Override
        public String getDefaultDatabase() {
            return "default";
        }

        @Override
        public boolean databaseExists(String databaseName) {
            return false;
        }

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public List<String> listTables(String databaseName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(TablePath tablePath) {
            return false;
        }

        @Override
        public CatalogTable getTable(TablePath tablePath) {
            throw new UnsupportedOperationException("not required for test");
        }

        @Override
        public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
                throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {}

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {}

        @Override
        public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
                throws DatabaseAlreadyExistException, CatalogException {}

        @Override
        public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
                throws DatabaseNotExistException, CatalogException {}
    }
}

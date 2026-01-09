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
package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue.Level;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTORIZATION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTOR_DIMENSIONS;

public class ElasticsearchSinkEnhancedValidatorTest {

    @Test
    public void shouldWarnWhenVersionLowerThanRequirement() {
        ElasticsearchSinkEnhancedValidator validator =
                new TestElasticsearchValidator("es-sink", "7.2.1");
        Map<String, Object> config = new HashMap<>();
        config.put(VECTORIZATION_FIELDS.key(), Arrays.asList("embedding"));
        config.put(VECTOR_DIMENSIONS.key(), 128);

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(2, issues.size());

        VersionCompatibilityConfigurationIssue vectorFieldIssue =
                findIssue(
                        issues,
                        VersionCompatibilityConfigurationIssue.class,
                        "vectorization_fields");
        Assertions.assertEquals(Level.WARNING, vectorFieldIssue.getLevel());
        Assertions.assertEquals("es-sink", vectorFieldIssue.getIdentifier());
        Assertions.assertEquals(PluginType.SINK, vectorFieldIssue.getPluginType());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'vectorization_fields' requires version '7.3+' (current version '7.2.1') in sink plugin 'es-sink'",
                vectorFieldIssue.getLog());

        VersionCompatibilityConfigurationIssue vectorDimensionIssue =
                findIssue(
                        issues, VersionCompatibilityConfigurationIssue.class, "vector_dimensions");
        Assertions.assertEquals(Level.WARNING, vectorDimensionIssue.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'vector_dimensions' requires version '7.3+' (current version '7.2.1') in sink plugin 'es-sink'",
                vectorDimensionIssue.getLog());
    }

    @Test
    public void shouldPassWhenVersionIsCompatible() {
        ElasticsearchSinkEnhancedValidator validator =
                new TestElasticsearchValidator("es-sink", "7.3.0");
        Map<String, Object> config = new HashMap<>();
        config.put(VECTORIZATION_FIELDS.key(), Arrays.asList("vector"));
        config.put(VECTOR_DIMENSIONS.key(), 64);

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertTrue(issues.isEmpty());
    }

    private <T extends ConfigurationVerificationIssue> T findIssue(
            List<ConfigurationVerificationIssue> issues, Class<T> clazz, String keyFragment) {
        return clazz.cast(
                issues.stream()
                        .filter(clazz::isInstance)
                        .filter(issue -> issue.getLog().contains("'" + keyFragment + "'"))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected issue "
                                                        + clazz.getSimpleName()
                                                        + " containing "
                                                        + keyFragment)));
    }

    private static class TestElasticsearchValidator extends ElasticsearchSinkEnhancedValidator {

        private TestElasticsearchValidator(String identifier, String version) {
            super(identifier, Optional.of(new FakeCatalog(version)));
        }
    }

    private static class FakeCatalog implements Catalog {

        private final String version;

        private FakeCatalog(String version) {
            this.version = version;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public String name() {
            return "fake-es-catalog";
        }

        @Override
        public Optional<String> getServiceVersion() {
            return Optional.ofNullable(version);
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
            throw new UnsupportedOperationException("not required for validator test");
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

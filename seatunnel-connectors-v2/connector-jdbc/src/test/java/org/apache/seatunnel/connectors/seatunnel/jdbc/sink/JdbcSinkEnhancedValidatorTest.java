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
package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue.Level;
import org.apache.seatunnel.api.configuration.util.issue.ConflictConfigurationIssue;
import org.apache.seatunnel.api.configuration.util.issue.DeprecatedConfigurationIssue;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.IS_EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.TABLE_PREFIX;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.TABLE_SUFFIX;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.USE_COPY_STATEMENT;

public class JdbcSinkEnhancedValidatorTest {

    private final JdbcSinkEnhancedValidator validator = new JdbcSinkEnhancedValidator("jdbc-sink");

    @Test
    public void shouldCollectDeprecatedAndConflictIssues() {
        Map<String, Object> config = new HashMap<>();
        config.put(TABLE_PREFIX.key(), "pre_");
        config.put(TABLE_SUFFIX.key(), "_suf");
        config.put(URL.key(), "jdbc:mysql://localhost:5432/demo");
        config.put(USE_COPY_STATEMENT.key(), true);
        config.put(IS_EXACTLY_ONCE.key(), true);
        config.put(AUTO_COMMIT.key(), true);

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertEquals(4, issues.size());

        DeprecatedConfigurationIssue prefixIssue =
                findIssue(issues, DeprecatedConfigurationIssue.class, "tablePrefix");
        Assertions.assertEquals(Level.WARNING, prefixIssue.getLevel());
        Assertions.assertEquals("jdbc-sink", prefixIssue.getIdentifier());
        Assertions.assertEquals(PluginType.SINK, prefixIssue.getPluginType());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Deprecated configuration option 'tablePrefix' detected in sink plugin 'jdbc-sink'",
                prefixIssue.getLog());

        DeprecatedConfigurationIssue suffixIssue =
                findIssue(issues, DeprecatedConfigurationIssue.class, "tableSuffix");
        Assertions.assertEquals(Level.WARNING, suffixIssue.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Deprecated configuration option 'tableSuffix' detected in sink plugin 'jdbc-sink'",
                suffixIssue.getLog());

        ConflictConfigurationIssue copyConflict =
                findIssue(issues, ConflictConfigurationIssue.class, "use_copy_statement");
        Assertions.assertEquals(Level.ERROR, copyConflict.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'url' with value 'jdbc:mysql://localhost:5432/demo' conflicts with option 'use_copy_statement' (value 'true') in sink plugin 'jdbc-sink'",
                copyConflict.getLog());

        ConflictConfigurationIssue exactlyOnceConflict =
                findIssue(issues, ConflictConfigurationIssue.class, "is_exactly_once");
        Assertions.assertEquals(Level.WARNING, exactlyOnceConflict.getLevel());
        Assertions.assertEquals(
                "[SeaTunnel Config Validation] Configuration option 'is_exactly_once' with value 'true' conflicts with option 'auto_commit' (value 'true') in sink plugin 'jdbc-sink'",
                exactlyOnceConflict.getLog());
    }

    @Test
    public void shouldSkipCopyConflictWhenPostgresFamily() {
        Map<String, Object> config = new HashMap<>();
        config.put(URL.key(), "jdbc:postgresql://localhost:5432/demo");
        config.put(USE_COPY_STATEMENT.key(), true);

        List<ConfigurationVerificationIssue> issues =
                validator.validate(ReadonlyConfig.fromMap(config));

        Assertions.assertTrue(issues.isEmpty());
    }

    @Test
    public void shouldSkipExactlyOnceConflictWhenAutoCommitDisabled() {
        Map<String, Object> config = new HashMap<>();
        config.put(IS_EXACTLY_ONCE.key(), true);
        config.put(AUTO_COMMIT.key(), false);

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
}

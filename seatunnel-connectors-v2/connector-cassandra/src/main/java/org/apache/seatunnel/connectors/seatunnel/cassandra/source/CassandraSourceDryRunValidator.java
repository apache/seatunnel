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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraTableConfig;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Infers the runtime schema from prepared SELECT metadata, never from query results. */
final class CassandraSourceDryRunValidator {
    private static final Pattern SELECT = Pattern.compile("(?is)^\\s*SELECT\\s+.+");
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private CassandraSourceDryRunValidator() {}

    static List<CatalogTable> inferSchema(ReadonlyConfig config) throws InterruptedException {
        checkInterrupted();
        ConfigValidator.of(config).validate(new CassandraSourceFactory().optionRule());
        List<String> queries = new ArrayList<>();
        if (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            for (Map<String, Object> table : config.get(ConnectorCommonOptions.TABLE_CONFIGS)) {
                queries.add(ReadonlyConfig.fromMap(table).get(CassandraSourceOptions.CQL));
            }
        } else {
            queries.add(config.get(CassandraSourceOptions.CQL));
        }
        for (String query : queries) {
            if (!SELECT.matcher(query).matches()) {
                throw new IllegalArgumentException(
                        "Cassandra connect dry-run supports only CQL beginning with SELECT (optionally preceded by whitespace)");
            }
        }
        CassandraParameters parameters = new CassandraParameters();
        parameters.buildWithConfig(config);
        try (DriverConfigLoader loader = configLoader();
                CqlSession session =
                        CassandraClient.getCqlSessionBuilder(
                                        parameters.getHost(),
                                        parameters.getKeyspace(),
                                        parameters.getUsername(),
                                        parameters.getPassword(),
                                        parameters.getDatacenter())
                                .withConfigLoader(loader)
                                .build()) {
            List<CatalogTable> tables = new ArrayList<>();
            Set<String> tableIds = new HashSet<>();
            for (String query : queries) {
                checkInterrupted();
                PreparedStatement prepared =
                        session.prepare(
                                SimpleStatement.builder(query)
                                        .setConsistencyLevel(parameters.getConsistencyLevel())
                                        .setTimeout(TIMEOUT)
                                        .build());
                if (prepared.getVariableDefinitions().size() != 0) {
                    throw new SafeValidationException("Unbound parameters are not supported");
                }
                CassandraTableConfig table =
                        CassandraSource.buildTableConfig(
                                query,
                                prepared.getResultSetDefinitions(),
                                parameters.getKeyspace());
                if (!tableIds.add(table.getTableId())) {
                    throw new SafeValidationException("Duplicate table identifiers");
                }
                tables.add(table.getCatalogTable());
            }
            return tables;
        } catch (SafeValidationException failure) {
            checkInterrupted();
            // Closing the session may attach sensitive suppressed exceptions.
            throw new IllegalArgumentException(failure.getMessage());
        } catch (RuntimeException failure) {
            checkInterrupted();
            // CQL and server errors may contain literal values or credentials.
            throw new IllegalStateException(
                    "Cassandra connect dry-run failed. Check connection, credentials, keyspace, SELECT queries, supported types and unique table identifiers.");
        }
    }

    private static DriverConfigLoader configLoader() {
        OptionsMap limits = new OptionsMap();
        limits.put(TypedDriverOption.RECONNECT_ON_INIT, false);
        limits.put(TypedDriverOption.PREPARE_ON_ALL_NODES, false);
        limits.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, TIMEOUT);
        limits.put(TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, TIMEOUT);
        limits.put(TypedDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, TIMEOUT);
        limits.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, TIMEOUT);
        limits.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, TIMEOUT);
        limits.put(TypedDriverOption.REQUEST_TIMEOUT, TIMEOUT);
        return DriverConfigLoader.compose(
                DriverConfigLoader.fromMap(limits),
                DriverConfigLoader.fromDefaults(Thread.currentThread().getContextClassLoader()));
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Cassandra connect dry-run interrupted");
        }
    }

    private static final class SafeValidationException extends IllegalArgumentException {
        private SafeValidationException(String message) {
            super(message);
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.neo4j;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.sink.Neo4jSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.neo4j.source.Neo4jSource;
import org.apache.seatunnel.connectors.seatunnel.neo4j.source.Neo4jSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Neo4jFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new Neo4jSourceFactory()).optionRule());
        Assertions.assertNotNull((new Neo4jSinkFactory()).optionRule());
    }

    @Test
    void sourceOptionRuleAcceptsSingleTableConfig() {
        Map<String, Object> config = sourceConnectionConfig();
        config.put("query", "MATCH (p:Person) RETURN p.name");
        config.put("schema", schema("people"));

        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    @Test
    void sourceOptionRuleAcceptsTablesConfigs() {
        Map<String, Object> config = sourceConnectionConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        tableConfig("people", "MATCH (p:Person) RETURN p.name"),
                        tableConfig("companies", "MATCH (c:Company) RETURN c.name")));

        Assertions.assertDoesNotThrow(() -> validateSource(config));
    }

    @Test
    void sourceOptionRuleRejectsSingleAndMultiTableConfigTogether() {
        Map<String, Object> config = sourceConnectionConfig();
        config.put("query", "MATCH (p:Person) RETURN p.name");
        config.put("schema", schema("people"));
        config.put(
                "tables_configs",
                Collections.singletonList(
                        tableConfig("companies", "MATCH (c:Company) RETURN c.name")));

        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @Test
    void sourceOptionRuleRejectsMissingTableConfiguration() {
        Assertions.assertThrows(
                OptionValidationException.class, () -> validateSource(sourceConnectionConfig()));
    }

    @Test
    void sourceOptionRuleRejectsRootSchemaWithTablesConfigs() {
        Map<String, Object> config = sourceConnectionConfig();
        config.put("schema", schema("people"));
        config.put(
                "tables_configs",
                Collections.singletonList(
                        tableConfig("companies", "MATCH (c:Company) RETURN c.name")));

        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(config));
    }

    @Test
    void sourceOptionRuleRejectsInvalidTablesConfigs() {
        Map<String, Object> empty = sourceConnectionConfig();
        empty.put("tables_configs", Collections.emptyList());

        Map<String, Object> missingQuery = sourceConnectionConfig();
        Map<String, Object> tableWithoutQuery = new HashMap<>();
        tableWithoutQuery.put("schema", schema("people"));
        missingQuery.put("tables_configs", Collections.singletonList(tableWithoutQuery));

        Map<String, Object> missingSchema = sourceConnectionConfig();
        Map<String, Object> tableWithoutSchema = new HashMap<>();
        tableWithoutSchema.put("query", "MATCH (p:Person) RETURN p.name");
        missingSchema.put("tables_configs", Collections.singletonList(tableWithoutSchema));

        Map<String, Object> blankTable = sourceConnectionConfig();
        blankTable.put(
                "tables_configs",
                Collections.singletonList(tableConfig(" ", "MATCH (p:Person) RETURN p.name")));

        Map<String, Object> duplicateTable = sourceConnectionConfig();
        duplicateTable.put(
                "tables_configs",
                Arrays.asList(
                        tableConfig("people", "MATCH (p:Person) RETURN p.name"),
                        tableConfig("people", "MATCH (p:Person) RETURN p.name")));

        Assertions.assertAll(
                () ->
                        Assertions.assertThrows(
                                OptionValidationException.class, () -> validateSource(empty)),
                () ->
                        Assertions.assertThrows(
                                OptionValidationException.class,
                                () -> validateSource(missingQuery)),
                () ->
                        Assertions.assertThrows(
                                OptionValidationException.class,
                                () -> validateSource(missingSchema)),
                () ->
                        Assertions.assertThrows(
                                OptionValidationException.class, () -> validateSource(blankTable)),
                () ->
                        Assertions.assertThrows(
                                OptionValidationException.class,
                                () -> validateSource(duplicateTable)));
    }

    @Test
    void sourceFactoryProducesCatalogTableForEachQuery() {
        Map<String, Object> config = sourceConnectionConfig();
        config.put(
                "tables_configs",
                Arrays.asList(
                        tableConfig("people", "MATCH (p:Person) RETURN p.name"),
                        tableConfig("companies", "MATCH (c:Company) RETURN c.name")));
        validateSource(config);

        Object createdSource =
                new Neo4jSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(config),
                                        getClass().getClassLoader()))
                        .createSource();
        Neo4jSource source = (Neo4jSource) createdSource;

        List<CatalogTable> tables = source.getProducedCatalogTables();
        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals("people", tables.get(0).getTableId().toTablePath().toString());
        Assertions.assertEquals("companies", tables.get(1).getTableId().toTablePath().toString());
    }

    private static Map<String, Object> sourceConnectionConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("uri", "neo4j://localhost:7687");
        config.put("database", "neo4j");
        config.put("username", "neo4j");
        config.put("password", "password");
        return config;
    }

    private static Map<String, Object> tableConfig(String table, String query) {
        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("query", query);
        tableConfig.put("schema", schema(table));
        return tableConfig;
    }

    private static Map<String, Object> schema(String table) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "STRING");

        Map<String, Object> schema = new HashMap<>();
        schema.put("table", table);
        schema.put("fields", fields);
        return schema;
    }

    private static void validateSource(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new Neo4jSourceFactory().optionRule());
    }
}

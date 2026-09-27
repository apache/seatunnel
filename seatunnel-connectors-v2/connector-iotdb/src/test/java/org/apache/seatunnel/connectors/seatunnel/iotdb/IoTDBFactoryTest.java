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

package org.apache.seatunnel.connectors.seatunnel.iotdb;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.iotdb.sink.IoTDBSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSource;
import org.apache.seatunnel.connectors.seatunnel.iotdb.source.IoTDBSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IoTDBFactoryTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "tables_configs = []",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}, {sql = y, schema {table = \"a.\", fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, node_urls = \"other:6667\", schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, num_partitions = 2, lower_bound = -9223372036854775808, upper_bound = -1, schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{}]",
                "tables_configs = [{sql = \" \" , schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, schema {fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}, {sql = y, schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}]\nsql = root_query",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}]\nschema {fields {ts = bigint}}",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}]\nnum_partitions = 2",
                "tables_configs = [{sql = x, num_partitions = 2, schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, num_partitions = 0, lower_bound = 1, upper_bound = 10, schema {table = a, fields {ts = bigint}}}]",
                "tables_configs = [{sql = x, num_partitions = 2, lower_bound = 10, upper_bound = 1, schema {table = a, fields {ts = bigint}}}]",
                "sql = x"
            })
    void rejectsInvalidTableConfiguration(String options) {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "node_urls = \"localhost:6667\"\nusername = root\npassword = root\n"
                                        + options));
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(config).validate(new IoTDBSourceFactory().optionRule()));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "sql = x\nschema {fields {ts = bigint}}",
                "tables_configs = [{sql = x, schema {table = a, fields {ts = bigint}}}]"
            })
    void acceptsSingleTableAndLegacyConfiguration(String options) {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "node_urls = \"localhost:6667\"\nusername = root\npassword = root\n"
                                        + options));
        ConfigValidator.of(config).validate(new IoTDBSourceFactory().optionRule());
    }

    @Test
    void sourceFactoryProducesIndependentTables() {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "node_urls = \"localhost:6667\"\nusername = root\npassword = root\n"
                                        + "tables_configs = ["
                                        + "{sql = \"select temperature from root.weather\", schema {table = weather, fields {ts = bigint, temperature = float}}},"
                                        + "{sql = \"select enabled from root.status\", schema {table = status, fields {ts = bigint, enabled = boolean}}}"
                                        + "]"));
        IoTDBSourceFactory factory = new IoTDBSourceFactory();
        ConfigValidator.of(config).validate(factory.optionRule());
        Object createdSource =
                factory.createSource(
                                new TableSourceFactoryContext(config, getClass().getClassLoader()))
                        .createSource();
        IoTDBSource source = (IoTDBSource) createdSource;
        Assertions.assertEquals(2, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "weather", source.getProducedCatalogTables().get(0).getTablePath().toString());
        Assertions.assertEquals(
                "status", source.getProducedCatalogTables().get(1).getTablePath().toString());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new IoTDBSourceFactory()).optionRule());
        Assertions.assertNotNull((new IoTDBSinkFactory()).optionRule());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SELECT value FROM root.test LIMIT 1",
                "SELECT value FROM root.test -- comment",
                "SELECT value FROM root.test /* comment */",
                "SELECT count(value) FROM root.test",
                "SELECT value FROM (SELECT value FROM root.test)",
                "SELECT value FROM root.test WHERE name = 'where'",
                "DELETE FROM root.test"
            })
    void partitionedTablesRejectQueriesThatCannotBeSafelySplit(String sql) {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(
                        ConfigFactory.parseString(
                                "node_urls = \"localhost:6667\"\nusername = root\npassword = root\n"
                                        + "tables_configs = [{sql = \""
                                        + sql
                                        + "\", lower_bound = 0, upper_bound = 10, num_partitions = 2, schema {table = a, fields {ts = bigint, value = int}}}]"));
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(config).validate(new IoTDBSourceFactory().optionRule()));
    }
}

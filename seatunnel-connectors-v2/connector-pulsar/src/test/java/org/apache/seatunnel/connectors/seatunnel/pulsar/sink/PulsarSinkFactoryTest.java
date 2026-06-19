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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarSinkFactoryTest {

    /** Guards the option metadata that connector specification checks validate in CI. */
    @Test
    public void testOptionRuleContainsMultiTableReplica() {
        PulsarSinkFactory factory = new PulsarSinkFactory();
        OptionRule optionRule = factory.optionRule();

        assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA));
    }

    /** Verifies single-table Pulsar sinks still reject a missing topic. */
    @Test
    public void testCreateSinkRequiresTopicForSingleTable() {
        PulsarSinkFactory factory = new PulsarSinkFactory();

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        factory.createSink(
                                new TableSinkFactoryContext(
                                        getCatalogTable(), config(), getClass().getClassLoader())));
    }

    /** Verifies multi-table Pulsar sinks can derive topics without a single-table topic option. */
    @Test
    public void testCreateSinkAllowsMissingTopicForMultiTable() {
        PulsarSinkFactory factory = new PulsarSinkFactory();

        assertDoesNotThrow(
                () ->
                        factory.createSink(
                                new TableSinkFactoryContext(
                                        null, config(), getClass().getClassLoader())));
    }

    /** Builds the minimum valid configuration shared by the Pulsar sink factory tests. */
    private ReadonlyConfig config() {
        Map<String, Object> options = new HashMap<>();
        options.put("client.service-url", "pulsar://localhost:6650");
        options.put("admin.service-url", "http://localhost:8080");
        options.put("format", "json");
        options.put("field_delimiter", ",");
        options.put("semantics", "NON");
        options.put("message.routing.mode", "RoundRobinPartition");
        return ReadonlyConfig.fromMap(options);
    }

    /** Creates a single-table catalog context so topic validation follows the production path. */
    private CatalogTable getCatalogTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.INT_TYPE)
                        .nullable(true)
                        .build());
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "pulsar_table"),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "Pulsar table");
    }
}

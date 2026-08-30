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

package org.apache.seatunnel.connectors.seatunnel.snmp.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.snmp.config.SnmpSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class SnmpSinkFactoryTest {

    @Test
    void testFactoryIdentityAndOptions() {
        SnmpSinkFactory factory = new SnmpSinkFactory();

        Assertions.assertEquals(SnmpSinkOptions.CONNECTOR_IDENTITY, factory.factoryIdentifier());
        OptionRule rule = factory.optionRule();
        List<Option<?>> required =
                rule.getRequiredOptions().stream()
                        .flatMap(group -> group.getOptions().stream())
                        .collect(Collectors.toList());
        Assertions.assertTrue(required.contains(SnmpSinkOptions.HOST));
        Assertions.assertTrue(required.contains(SnmpSinkOptions.COMMUNITY));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.PORT));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.TIMEOUT_MILLIS));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.RETRIES));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.OID_FIELD));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.VALUE_FIELD));
        Assertions.assertTrue(rule.getOptionalOptions().contains(SnmpSinkOptions.VALUE_TYPE_FIELD));
    }

    @Test
    void testFactoryCreatesSerializableSinkWithSourceCompatibleSchema() {
        CatalogTable catalogTable = catalogTable();
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable,
                        ReadonlyConfig.fromMap(baseConfig()),
                        getClass().getClassLoader());

        SnmpSink sink = (SnmpSink) new SnmpSinkFactory().createSink(context).createSink();
        SnmpSink restored = SerializationUtils.deserialize(SerializationUtils.serialize(sink));

        Assertions.assertEquals("SNMP", restored.getPluginName());
        Assertions.assertArrayEquals(
                catalogTable.getSeaTunnelRowType().getFieldNames(),
                restored.getWriteCatalogTable()
                        .orElseThrow(() -> new AssertionError("Sink catalog table is missing"))
                        .getSeaTunnelRowType()
                        .getFieldNames());
    }

    private static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "agent", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "oid", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "value", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "value_type", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "poll_time", BasicType.LONG_TYPE, 0, false, null, null))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("default", "default", "snmp_sink_test"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "SNMP sink test table");
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "unit-test-community");
        return values;
    }
}

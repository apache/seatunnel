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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSink;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class JdbcFactoryTest {

    @Test
    void optionRule() {
        JdbcSourceFactory jdbcSourceFactory = new JdbcSourceFactory();
        Assertions.assertNotNull(jdbcSourceFactory.optionRule());
        Assertions.assertNotNull((new JdbcSinkFactory()).optionRule());

        Class<? extends SeaTunnelSource> sourceClass = jdbcSourceFactory.getSourceClass();
        Assertions.assertTrue(SupportParallelism.class.isAssignableFrom(sourceClass));
    }

    @Test
    void testSinkCatalogTable() {
        TableSinkFactoryContext tableSinkFactoryContext =
                new TableSinkFactoryContext(
                        getSimpleCatalogTable(),
                        getSimpleReadonlyConfig(),
                        Thread.currentThread().getContextClassLoader());
        JdbcSinkFactory jdbcSinkFactory = new JdbcSinkFactory();
        final SeaTunnelSink sink = jdbcSinkFactory.createSink(tableSinkFactoryContext).createSink();
        JdbcSink jdbcSink = (JdbcSink) sink;
        Assertions.assertTrue(jdbcSink.getWriteCatalogTable().isPresent());
        Assertions.assertNull(
                jdbcSink.getWriteCatalogTable().get().getTableSchema().getPrimaryKey());
    }

    private ReadonlyConfig getSimpleReadonlyConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true");
        map.put("driver", "com.mysql.cj.jdbc.Driver");
        map.put("user", "root");
        map.put("password", "12345");
        map.put("database", "test");
        map.put("table", "test_table");
        map.put("primary_keys", "[]");
        return ReadonlyConfig.fromMap(map);
    }

    private CatalogTable getSimpleCatalogTable() {
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "test", BasicType.STRING_TYPE, 1L, true, null, ""))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "comment");
    }
}

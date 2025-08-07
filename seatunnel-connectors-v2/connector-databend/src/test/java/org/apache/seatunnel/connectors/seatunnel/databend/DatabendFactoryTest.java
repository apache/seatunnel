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

package org.apache.seatunnel.connectors.seatunnel.databend;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.databend.sink.DatabendSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.databend.source.DatabendSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabendFactoryTest {

    @Test
    public void testOptionRule() {
        DatabendSourceFactory sourceFactory = new DatabendSourceFactory();
        DatabendSinkFactory sinkFactory = new DatabendSinkFactory();

        OptionRule sourceOptionRule = sourceFactory.optionRule();
        OptionRule sinkOptionRule = sinkFactory.optionRule();

        Assertions.assertNotNull(sourceOptionRule);
        Assertions.assertNotNull(sinkOptionRule);
    }

    @Test
    public void testCreateSource() {
        DatabendSourceFactory sourceFactory = new DatabendSourceFactory();

        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:databend://localhost:8000");
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "default");
        options.put("table", "test");

        TableSourceFactoryContext context = getTableSourceFactoryContext(options);

        Assertions.assertNotNull(sourceFactory.createSource(context));
    }

    @Test
    public void testCreateSink() {
        DatabendSinkFactory sinkFactory = new DatabendSinkFactory();

        Map<String, Object> options = new HashMap<>();
        options.put("url", "jdbc:databend://localhost:8000");
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "default");
        options.put("table", "test");
        options.put("batch_size", "2000");

        TableSinkFactoryContext context = getTableSinkFactoryContext(options);

        Assertions.assertNotNull(sinkFactory.createSink(context));
    }

    private TableSourceFactoryContext getTableSourceFactoryContext(Map<String, Object> options) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        return new TableSourceFactoryContext(
                config, Thread.currentThread().getContextClassLoader());
    }

    private TableSinkFactoryContext getTableSinkFactoryContext(Map<String, Object> options) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        return new TableSinkFactoryContext(
                getCatalogTable(), config, Thread.currentThread().getContextClassLoader());
    }

    private CatalogTable getCatalogTable() {
        SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.DOUBLE_TYPE
        };
        String[] fieldNames = {"name", "age", "score"};

        // create columns
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Column column =
                    PhysicalColumn.builder()
                            .name(fieldNames[i])
                            .dataType(fieldTypes[i])
                            .nullable(true)
                            .build();
            columns.add(column);
        }

        // create table schema
        TableSchema tableSchema = TableSchema.builder().columns(columns).build();

        Map<String, String> options = new HashMap<>();
        List<String> partitionKeys = new ArrayList<>();

        return CatalogTable.of(
                TableIdentifier.of("default", "test", "test"),
                tableSchema,
                options,
                partitionKeys,
                "Test Databend Table");
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.lance.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.catalog.LanceCatalog;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSinkConfig;

import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LanceSinkTest {

    private LanceCatalog lanceCatalog;

    private TableSchema.Builder schemaBuilder;

    private final String CATALOG_NAME = "lance_namespace";

    private final String DATABASE_NAME = "default";

    private final String TABLE_NAME = "test_table3";

    private LanceSinkWriter sinkWriter;

    private ReadonlyConfig readonlyConfig;

    @BeforeEach
    public void before() {
        Map<String, Object> configs = new HashMap<>();
        String testDir = System.getProperty("java.io.tmpdir");
        String fullDatasetPath = testDir + "/test/" + TABLE_NAME + ".lance";
        configs.put(LanceCommonOptions.KEY_DATASET_PATH.key(), fullDatasetPath);
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        readonlyConfig = ReadonlyConfig.fromMap(configs);
        lanceCatalog = new LanceCatalog(CATALOG_NAME, readonlyConfig);
        lanceCatalog.open();

        this.schemaBuilder =
                TableSchema.builder()
                        // TODO: support map/array
                        .column(
                                PhysicalColumn.of(
                                        "c_string",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_string"))
                        .column(
                                PhysicalColumn.of(
                                        "c_boolean",
                                        BasicType.BOOLEAN_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_boolean"))
                        .column(
                                PhysicalColumn.of(
                                        "c_tinyint",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_tinyint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_smallint",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_smallint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_int",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_int"))
                        .column(
                                PhysicalColumn.of(
                                        "c_bigint",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bigint"))
                        .column(
                                PhysicalColumn.of(
                                        "c_float",
                                        BasicType.FLOAT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_float"))
                        .column(
                                PhysicalColumn.of(
                                        "c_double",
                                        BasicType.DOUBLE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_double"))
                        // TODO: solve decimal trans problem
                        .column(
                                PhysicalColumn.of(
                                        "c_bytes",
                                        PrimitiveByteArrayType.INSTANCE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bytes"));
        // TODO: support date/time/timestamp

        lanceCatalog.createTable(
                TablePath.of(DATABASE_NAME, TABLE_NAME),
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                        schemaBuilder.build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test table"),
                false);

        TableSchema tableSchema = schemaBuilder.build();
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        LanceSinkConfig sinkConfig = new LanceSinkConfig(readonlyConfig);
        LanceCatalog catalog = new LanceCatalog(CATALOG_NAME, readonlyConfig);
        sinkWriter = new LanceSinkWriter(rowType, tableSchema, sinkConfig, catalog);

        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("key1", "value1");
        mapValue.put("key2", "value2");

        Object[] fields =
                new Object[] {
                    // mapValue, // c_map
                    // Arrays.asList("item1", "item2", "item3").toArray(new String[0]), // c_array
                    "test_string", // c_string
                    true, // c_boolean
                    1, // c_tinyint
                    2, // c_smallint
                    3, // c_int
                    4L, // c_bigint
                    5.0f, // c_float
                    6.0, // c_double
                    // new BigDecimal("123.45"), // c_decimal
                    new byte[] {1, 2, 3} // c_bytes
                    // LocalDate.of(2024, 12, 28), // c_date
                    // LocalDateTime.of(2024, 12, 28, 10, 30, 0), // c_timestamp
                    // LocalTime.of(10, 30, 0) // c_time
                };
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);

        try {
            sinkWriter.write(seaTunnelRow);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

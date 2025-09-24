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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
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

    private final String TABLE_NAME = "test_table";

    private LanceSinkWriter sinkWriter;

    private ReadonlyConfig readonlyConfig;

    @BeforeEach
    public void before() {
        Map<String, Object> configs = new HashMap<>();
        // build catalog configs
        configs.put(
                LanceCommonOptions.KEY_DATASET_PATH.key(),
                "/Users/silenceland/Documents/develop/test");
        configs.put(LanceCommonOptions.KEY_NAMESPACE_TYPE.key(), "dir");
        readonlyConfig = ReadonlyConfig.fromMap(configs);
        lanceCatalog = new LanceCatalog(CATALOG_NAME, readonlyConfig);
        lanceCatalog.open();

        this.schemaBuilder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c_map",
                                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "c_array",
                                        ArrayType.STRING_ARRAY_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_array"))
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
                        .column(
                                PhysicalColumn.of(
                                        "c_decimal",
                                        new DecimalType(10, 2),
                                        (Long) null,
                                        false,
                                        null,
                                        "c_decimal"))
                        .column(
                                PhysicalColumn.of(
                                        "c_bytes",
                                        BasicType.BYTE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bytes"))
                        .column(
                                PhysicalColumn.of(
                                        "c_date",
                                        LocalTimeType.LOCAL_DATE_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_date"))
                        .column(
                                PhysicalColumn.of(
                                        "c_timestamp",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_timestamp"))
                        .column(
                                PhysicalColumn.of(
                                        "c_time",
                                        LocalTimeType.LOCAL_TIME_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_time"));

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

        Object[] fields = new Object[] {};
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);

        try {
            sinkWriter.write(seaTunnelRow);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

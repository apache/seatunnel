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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.writer;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
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
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSinkWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PaimonWriteTest {

    private PaimonCatalog paimonCatalog;
    private TableSchema.Builder schemaBuilder;
    private final String CATALOG_NAME = "paimon_catalog";
    private final String DATABASE_NAME = "test_default";
    private final String TABLE_NAME = "test_table";
    private PaimonSinkWriter paimonSinkWriter;
    private ReadonlyConfig readonlyConfig;
    private SinkWriter.Context context;

    @BeforeEach
    public void before() {

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/paimon");
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("write-only", "true");
        properties.put("paimon.table.write-props", writeProps);
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonCatalog = new PaimonCatalog(CATALOG_NAME, readonlyConfig);
        paimonCatalog.open();
        paimonCatalog.createDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), false);
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
        paimonCatalog.createTable(
                TablePath.of(DATABASE_NAME, TABLE_NAME),
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                        schemaBuilder.build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test table"),
                false);

        context =
                new SinkWriter.Context() {
                    @Override
                    public int getIndexOfSubtask() {
                        return 0;
                    }

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };
    }

    @Test
    void testWaitCompaction() throws Exception {

        JobContext jobContext = new JobContext();
        jobContext.setJobMode(JobMode.STREAMING);
        TablePath tablePath = TablePath.of(DATABASE_NAME, TABLE_NAME);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration());
        Assertions.assertFalse(paimonSinkWriter.waitCompaction());

        jobContext.setJobMode(JobMode.BATCH);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/paimon");
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("changelog-producer", "lookup");
        properties.put("paimon.table.write-props", writeProps);
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());

        writeProps.put("changelog-producer", "full-compaction");
        readonlyConfig = ReadonlyConfig.fromMap(properties);
        paimonSinkWriter =
                new PaimonSinkWriter(
                        context,
                        readonlyConfig,
                        paimonCatalog.getTable(tablePath),
                        paimonCatalog.getPaimonTable(tablePath),
                        jobContext,
                        new PaimonSinkConfig(readonlyConfig),
                        new PaimonHadoopConfiguration());
        Assertions.assertTrue(paimonSinkWriter.waitCompaction());
    }

    @AfterEach
    public void after() {
        paimonCatalog.dropDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), false);
        paimonCatalog.close();
    }
}

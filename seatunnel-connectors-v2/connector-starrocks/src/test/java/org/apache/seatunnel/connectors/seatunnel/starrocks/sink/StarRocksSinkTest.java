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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

/**
 * Guards the StarRocks sink fail-fast for native JSON columns: CSV Stream Load cannot carry a JSON
 * logical column, so the combination must be rejected when the sink is created instead of at the
 * first flush, while the JSON Stream Load format keeps accepting the same schema.
 */
class StarRocksSinkTest {

    private static CatalogTable tableWithJsonColumn() {
        return CatalogTable.of(
                TableIdentifier.of("starrocks", "db", null, "t"),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, 20L, 0, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "payload", BasicType.JSON_TYPE, null, null, true, null, ""))
                        .build(),
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private static SinkConfig sinkConfig(SinkConfig.StreamLoadFormat loadFormat) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setDatabase("db");
        sinkConfig.setTable("t");
        sinkConfig.setLoadFormat(loadFormat);
        return sinkConfig;
    }

    @Test
    void csvStreamLoadWithJsonColumnFailsAtSinkCreation() {
        StarRocksConnectorException exception =
                Assertions.assertThrows(
                        StarRocksConnectorException.class,
                        () ->
                                new StarRocksSink(
                                        sinkConfig(SinkConfig.StreamLoadFormat.CSV),
                                        tableWithJsonColumn(),
                                        Collections.emptyMap()));
        Assertions.assertTrue(exception.getMessage().contains("payload"), exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("starrocks.config.format"), exception.getMessage());
    }

    @Test
    void jsonStreamLoadWithJsonColumnIsAccepted() {
        Assertions.assertDoesNotThrow(
                () ->
                        new StarRocksSink(
                                sinkConfig(SinkConfig.StreamLoadFormat.JSON),
                                tableWithJsonColumn(),
                                Collections.emptyMap()));
    }
}

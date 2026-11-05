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

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class WebSocketSourceTest {

    private static Map<String, Object> baseConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), "ws://localhost:8080/topic");
        return configMap;
    }

    private static WebSocketSource of(Map<String, Object> configMap, JobMode jobMode) {
        WebSocketSource source = new WebSocketSource(ReadonlyConfig.fromMap(configMap));
        source.setJobContext(new JobContext().setJobMode(jobMode));
        return source;
    }

    @Test
    void testPluginName() {
        Assertions.assertEquals("WebSocket", of(baseConfig(), JobMode.STREAMING).getPluginName());
    }

    @Test
    void testSingleValueColumnWithoutSchema() {
        List<CatalogTable> tables = of(baseConfig(), JobMode.STREAMING).getProducedCatalogTables();
        Assertions.assertEquals(1, tables.size());
        Assertions.assertArrayEquals(
                new String[] {"value"}, tables.get(0).getSeaTunnelRowType().getFieldNames());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, tables.get(0).getSeaTunnelRowType().getFieldType(0));
    }

    @Test
    void testSchemaIsUsedWhenConfigured() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(
                ConnectorCommonOptions.SCHEMA.key(),
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        CatalogTable catalogTable =
                of(configMap, JobMode.STREAMING).getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"id"}, catalogTable.getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void testStreamingJobIsUnbounded() {
        Assertions.assertEquals(
                Boundedness.UNBOUNDED, of(baseConfig(), JobMode.STREAMING).getBoundedness());
    }

    @Test
    void testBatchJobWithMaxRecordsIsBounded() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.MAX_RECORDS.key(), 10);
        Assertions.assertEquals(Boundedness.BOUNDED, of(configMap, JobMode.BATCH).getBoundedness());
    }

    @Test
    void testBatchJobWithReadTimeoutIsBounded() {
        Map<String, Object> configMap = baseConfig();
        configMap.put(WebSocketSourceOptions.READ_TIMEOUT_MS.key(), 5000);
        Assertions.assertEquals(Boundedness.BOUNDED, of(configMap, JobMode.BATCH).getBoundedness());
    }

    /** A batch job without a stop condition would never finish, so it must be rejected early. */
    @Test
    void testBatchJobWithoutStopConditionIsRejected() {
        WebSocketSource source = of(baseConfig(), JobMode.BATCH);
        WebSocketConnectorException exception =
                Assertions.assertThrows(WebSocketConnectorException.class, source::getBoundedness);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.CONFIG_VALIDATION_FAILED,
                exception.getSeaTunnelErrorCode());
    }
}

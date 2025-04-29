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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarRocksStreamLoadVisitorTest {

    @Test
    void throwsExceptionWhenBatchMaxBytesExceedsLimitForCSVFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.CSV);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483638L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(100);
        Map<String, Object> props = new HashMap<>();
        props.put("row_delimiter", "\n");
        when(sinkConfig.getStreamLoadProps()).thenReturn(props);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(2147483638L, 100);
                });
    }

    @Test
    void throwsExceptionWhenBatchMaxBytesExceedsLimitForJSONFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483637L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(100);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(2147483637L, 100);
                });
    }

    @Test
    void doesNotThrowExceptionWhenBatchMaxBytesWithinLimitForCSVFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.CSV);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483637L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        Map<String, Object> props = new HashMap<>();
        props.put("row_delimiter", "\n");
        when(sinkConfig.getStreamLoadProps()).thenReturn(props);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));

        assertDoesNotThrow(() -> visitor.checkBatchMaxBytes(2147483637L, 10));
    }

    @Test
    void doesNotThrowExceptionWhenBatchMaxBytesWithinLimitForJSONFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483636L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
        assertDoesNotThrow(() -> visitor.checkBatchMaxBytes(2147483636L, 10));
    }

    @Test
    void throwsExceptionForUnsupportedLoadFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(1024L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(1024, 10);
                });
    }
}

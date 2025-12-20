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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_COMMITTED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;

public class ConnectorMetricsCalcContextTest {

    private static final String TABLE_ID = "fake.table1";

    @Test
    public void testCommitFlushesPendingMetrics() {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        ConnectorMetricsCalcContext calcContext =
                new ConnectorMetricsCalcContext(
                        metricsContext,
                        PluginType.SINK,
                        true,
                        Collections.singletonList(TablePath.of(TABLE_ID)));

        SeaTunnelRow row = createRowWithTableId(TABLE_ID, "A");

        calcContext.updateMetrics(row, TABLE_ID);
        Assertions.assertEquals(1, metricsContext.counter(SINK_WRITE_COUNT).getCount());
        Assertions.assertEquals(
                1, metricsContext.counter(SINK_WRITE_COUNT + "#" + TABLE_ID).getCount());

        Assertions.assertEquals(0, metricsContext.counter(SINK_COMMITTED_COUNT).getCount());
        Assertions.assertEquals(
                0, metricsContext.counter(SINK_COMMITTED_COUNT + "#" + TABLE_ID).getCount());

        long checkpointId = 1L;
        calcContext.sealCheckpointMetrics(checkpointId);

        Assertions.assertEquals(0, metricsContext.counter(SINK_COMMITTED_COUNT).getCount());

        calcContext.commitPendingMetrics(checkpointId);

        Assertions.assertEquals(1, metricsContext.counter(SINK_COMMITTED_COUNT).getCount());
        Assertions.assertEquals(
                1, metricsContext.counter(SINK_COMMITTED_COUNT + "#" + TABLE_ID).getCount());

        Counter writeBytes = metricsContext.counter(SINK_WRITE_BYTES);
        Counter committedBytes = metricsContext.counter(SINK_COMMITTED_BYTES);
        Assertions.assertEquals(writeBytes.getCount(), committedBytes.getCount());
    }

    @Test
    public void testAbortClearsPendingMetrics() {
        SeaTunnelMetricsContext metricsContext = new SeaTunnelMetricsContext();
        ConnectorMetricsCalcContext calcContext =
                new ConnectorMetricsCalcContext(
                        metricsContext,
                        PluginType.SINK,
                        true,
                        Collections.singletonList(TablePath.of(TABLE_ID)));

        SeaTunnelRow row = createRowWithTableId(TABLE_ID, "B");

        calcContext.updateMetrics(row, TABLE_ID);
        Assertions.assertEquals(1, metricsContext.counter(SINK_WRITE_COUNT).getCount());

        long checkpointId = 2L;
        calcContext.sealCheckpointMetrics(checkpointId);
        calcContext.abortPendingMetrics(checkpointId);
        calcContext.commitPendingMetrics(checkpointId);

        Assertions.assertEquals(0, metricsContext.counter(SINK_COMMITTED_COUNT).getCount());
        Assertions.assertEquals(
                0, metricsContext.counter(SINK_COMMITTED_COUNT + "#" + TABLE_ID).getCount());
    }

    private SeaTunnelRow createRowWithTableId(String tableId, String payload) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, payload});
        row.setTableId(tableId);
        return row;
    }
}

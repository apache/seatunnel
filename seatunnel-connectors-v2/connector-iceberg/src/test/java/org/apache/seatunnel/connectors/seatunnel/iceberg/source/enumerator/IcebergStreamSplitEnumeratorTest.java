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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Minimal test for {@link IcebergStreamSplitEnumerator} wait / notify fix. */
class IcebergStreamSplitEnumeratorTest {

    @Test
    void testHandleSplitRequestDoesNotThrowIllegalMonitorStateException() throws Exception {
        SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> context =
                new DummyEnumeratorContext();

        IcebergSourceConfig sourceConfig = createSourceConfig();

        // Catalog tables must be non-empty because AbstractSplitEnumerator uses the size as the
        // capacity of an ArrayBlockingQueue.
        TablePath tablePath = TablePath.of("default", "source");
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("seatunnel", "default", "source"),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test table");
        Map<TablePath, CatalogTable> catalogTables =
                Collections.singletonMap(tablePath, catalogTable);

        IcebergStreamSplitEnumerator enumerator =
                new IcebergStreamSplitEnumerator(
                        context, sourceConfig, catalogTables, Collections.emptyMap());

        // Force initialized = true so handleSplitRequest executes the notify logic.
        enumerator.initialized = true;

        // Before the fix, this would throw IllegalMonitorStateException because notifyAll was
        // called without holding the monitor.
        Assertions.assertDoesNotThrow(() -> enumerator.handleSplitRequest(0));
    }

    private IcebergSourceConfig createSourceConfig() {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", "hadoop");
        catalogProps.put("warehouse", Paths.get("target", "iceberg", "hadoop").toUri().toString());

        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "default");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "source");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);

        return new IcebergSourceConfig(ReadonlyConfig.fromMap(configs));
    }

    private static class DummyEnumeratorContext
            implements SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> {

        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public java.util.Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtaskId, java.util.List<IcebergFileScanTaskSplit> splits) {
            // no-op
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            // no-op
        }

        @Override
        public void sendEventToSourceReader(
                int subtaskId, org.apache.seatunnel.api.source.SourceEvent event) {
            // no-op
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return eventListener;
        }
    }
}

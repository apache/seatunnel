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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Covers legacy-lane checkpoint restore of {@link FakeSourceReader}: a split restored with the
 * custom-row progress marker must resume after the rows emitted before the checkpoint instead of
 * replaying the whole custom row list.
 */
class FakeSourceReaderTest {

    @Test
    void legacyCustomRowProgressSurvivesRestore() throws Exception {
        MultipleTableFakeSourceConfig sourceConfig = loadConfig("/fake-data.schema.conf");
        String tableId =
                sourceConfig
                        .getFakeConfigs()
                        .get(0)
                        .getCatalogTable()
                        .getTableId()
                        .toTablePath()
                        .toString();
        FakeSourceReader reader =
                new FakeSourceReader(new TestingReaderContext(), sourceConfig, "job");

        // -3 is the legacy encoding of "the next custom row index is 2": rows 0 and 1 were
        // emitted before the checkpoint was taken.
        reader.addSplits(Collections.singletonList(new FakeSourceSplit(tableId, 0, -3)));
        reader.handleNoMoreSplits();
        Assertions.assertEquals(-3, reader.snapshotState(1L).get(0).getRowNum());

        TestingCollector collector = new TestingCollector();
        reader.pollNext(collector);

        // fake-data.schema.conf declares six custom rows; only rows 2..5 may be emitted.
        Assertions.assertEquals(4, collector.rows.size());
        Assertions.assertEquals(3L, collector.rows.get(0).getField(0));
        Assertions.assertTrue(reader.snapshotState(2L).isEmpty());
    }

    private static MultipleTableFakeSourceConfig loadConfig(String resourceName) throws Exception {
        Config config =
                ConfigFactory.parseFile(
                        new File(
                                Paths.get(
                                                FakeSourceReaderTest.class
                                                        .getResource(resourceName)
                                                        .toURI())
                                        .toString()));
        return new MultipleTableFakeSourceConfig(
                ReadonlyConfig.fromConfig(config.getConfig("FakeSource")));
    }

    private static final class TestingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    private static final class TestingReaderContext implements SourceReader.Context {
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }
}

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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TransformExecuteProcessorTest {

    @Test
    void mapFunctionPropagatesLifecycleExactlyOnce() {
        LifecycleTracker tracker = new LifecycleTracker();
        TransformExecuteProcessor.TransformMapFunction function =
                new TransformExecuteProcessor.TransformMapFunction(
                        new TrackingMapTransform(tracker));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});

        function.open(new Configuration());
        function.open(new Configuration());
        Assertions.assertSame(row, function.map(row));
        function.close();
        function.close();

        Assertions.assertEquals(1, tracker.openCount);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void flatMapFunctionPropagatesLifecycleExactlyOnce() throws Exception {
        LifecycleTracker tracker = new LifecycleTracker();
        TransformExecuteProcessor.ArrayFlatMap function =
                new TransformExecuteProcessor.ArrayFlatMap(new TrackingFlatMapTransform(tracker));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        List<SeaTunnelRow> output = new ArrayList<>();

        function.open(new Configuration());
        function.open(new Configuration());
        function.flatMap(row, new ListCollector<>(output));
        function.close();
        function.close();

        Assertions.assertEquals(Collections.singletonList(row), output);
        Assertions.assertEquals(1, tracker.openCount);
        Assertions.assertEquals(1, tracker.closeCount);
    }

    @Test
    void closeFailureDoesNotEscapeLifecycleCallback() {
        LifecycleTracker tracker = new LifecycleTracker();
        tracker.failOnClose = true;
        TransformExecuteProcessor.TransformMapFunction function =
                new TransformExecuteProcessor.TransformMapFunction(
                        new TrackingMapTransform(tracker));

        function.open(new Configuration());
        Assertions.assertDoesNotThrow(function::close);
        Assertions.assertDoesNotThrow(function::close);

        Assertions.assertEquals(1, tracker.closeCount);
    }

    private static class LifecycleTracker {
        private int openCount;
        private int closeCount;
        private boolean failOnClose;
    }

    private abstract static class TrackingTransform {
        protected final LifecycleTracker tracker;

        private TrackingTransform(LifecycleTracker tracker) {
            this.tracker = tracker;
        }

        public String getPluginName() {
            return "lifecycle-test";
        }

        public void open() {
            tracker.openCount++;
        }

        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        public void close() {
            tracker.closeCount++;
            if (tracker.failOnClose) {
                throw new RuntimeException("expected close failure");
            }
        }
    }

    private static class TrackingMapTransform extends TrackingTransform
            implements SeaTunnelMapTransform<SeaTunnelRow> {

        private TrackingMapTransform(LifecycleTracker tracker) {
            super(tracker);
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }
    }

    private static class TrackingFlatMapTransform extends TrackingTransform
            implements SeaTunnelFlatMapTransform<SeaTunnelRow> {

        private TrackingFlatMapTransform(LifecycleTracker tracker) {
            super(tracker);
        }

        @Override
        public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
            return Collections.singletonList(row);
        }
    }
}

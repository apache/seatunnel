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
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class FakeSourceSplitEnumeratorTest {

    @Test
    void signalNoMoreSplitsAfterRestoreWhenNoPendingSplits() throws Exception {
        MultipleTableFakeSourceConfig sourceConfig = loadSingleTableFakeSourceConfig();

        TestingEnumeratorContext firstContext =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        FakeSourceSplitEnumerator firstRunEnumerator =
                new FakeSourceSplitEnumerator(firstContext, sourceConfig, Collections.emptySet());
        firstRunEnumerator.run();

        Set<FakeSourceSplit> assignedSplits = new HashSet<>(firstContext.getAllAssignedSplits());
        Assertions.assertFalse(assignedSplits.isEmpty(), "Expected assigned splits in first run");

        TestingEnumeratorContext restoredContext =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        FakeSourceSplitEnumerator restoredEnumerator =
                new FakeSourceSplitEnumerator(restoredContext, sourceConfig, assignedSplits);
        restoredEnumerator.run();

        Assertions.assertTrue(
                restoredContext.getAllAssignedSplits().isEmpty(),
                "Expected no split assignments on restore when all splits were already assigned");
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(0, 1)),
                restoredContext.getNoMoreSplitsReaders(),
                "Expected signalNoMoreSplits for all registered readers");
    }

    @Test
    void assignAndSignalOnLateRegisterReaderAfterDiscovery() throws Exception {
        MultipleTableFakeSourceConfig sourceConfig = loadSingleTableFakeSourceConfig();

        TestingEnumeratorContext context = new TestingEnumeratorContext(2, new HashSet<>());
        FakeSourceSplitEnumerator enumerator =
                new FakeSourceSplitEnumerator(context, sourceConfig, Collections.emptySet());

        enumerator.run();
        Assertions.assertTrue(
                context.getAllAssignedSplits().isEmpty(),
                "Expected no split assignments when no readers are registered during run()");

        enumerator.registerReader(0);
        enumerator.registerReader(1);

        Assertions.assertFalse(
                context.getAllAssignedSplits().isEmpty(),
                "Expected split assignments after late reader registration");
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(0, 1)),
                context.getNoMoreSplitsReaders(),
                "Expected signalNoMoreSplits for late registered readers");
    }

    private static MultipleTableFakeSourceConfig loadSingleTableFakeSourceConfig()
            throws URISyntaxException {
        URL resource = FakeSourceSplitEnumeratorTest.class.getResource("/simple.schema.conf");
        Config config = ConfigFactory.parseFile(new File(Paths.get(resource.toURI()).toString()));
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config.getConfig("FakeSource"));
        return new MultipleTableFakeSourceConfig(readonlyConfig);
    }

    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<FakeSourceSplit> {
        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<FakeSourceSplit>> assignedSplitsByReader = new HashMap<>();
        private final Set<Integer> noMoreSplitsReaders = new HashSet<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        private TestingEnumeratorContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = registeredReaders;
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<FakeSourceSplit> splits) {
            assignedSplitsByReader
                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitsReaders.add(subtask);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
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

        private List<FakeSourceSplit> getAllAssignedSplits() {
            return assignedSplitsByReader.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }

        private Set<Integer> getNoMoreSplitsReaders() {
            return noMoreSplitsReaders;
        }
    }
}

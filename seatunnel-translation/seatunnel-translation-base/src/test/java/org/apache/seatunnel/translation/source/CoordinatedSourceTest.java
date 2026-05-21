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

package org.apache.seatunnel.translation.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class CoordinatedSourceTest {

    @Test
    void testCloseIgnoresReaderContextRemovedAfterCompletion() throws IOException {
        AtomicBoolean readerClosed = new AtomicBoolean(false);
        AtomicBoolean enumeratorClosed = new AtomicBoolean(false);
        CoordinatedSource<String, TestingSplit, Integer> coordinatedSource =
                new CoordinatedSource<>(
                        new TestingSource(readerClosed, enumeratorClosed), null, 1, "job-1");

        coordinatedSource.handleNoMoreElement(0);

        coordinatedSource.close();

        Assertions.assertTrue(readerClosed.get());
        Assertions.assertTrue(enumeratorClosed.get());
    }

    private static final class TestingSource
            implements SeaTunnelSource<String, TestingSplit, Integer>, Serializable {

        private final AtomicBoolean readerClosed;
        private final AtomicBoolean enumeratorClosed;

        private TestingSource(AtomicBoolean readerClosed, AtomicBoolean enumeratorClosed) {
            this.readerClosed = readerClosed;
            this.enumeratorClosed = enumeratorClosed;
        }

        @Override
        public String getPluginName() {
            return "testing-source";
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, TestingSplit> createReader(SourceReader.Context readerContext) {
            return new TestingReader(readerClosed);
        }

        @Override
        public SourceSplitEnumerator<TestingSplit, Integer> createEnumerator(
                SourceSplitEnumerator.Context<TestingSplit> enumeratorContext) {
            return new TestingEnumerator(enumeratorClosed);
        }

        @Override
        public SourceSplitEnumerator<TestingSplit, Integer> restoreEnumerator(
                SourceSplitEnumerator.Context<TestingSplit> enumeratorContext,
                Integer checkpointState) {
            return createEnumerator(enumeratorContext);
        }
    }

    private static final class TestingReader implements SourceReader<String, TestingSplit> {

        private final AtomicBoolean readerClosed;

        private TestingReader(AtomicBoolean readerClosed) {
            this.readerClosed = readerClosed;
        }

        @Override
        public void open() {}

        @Override
        public void close() {
            readerClosed.set(true);
        }

        @Override
        public void pollNext(Collector<String> output) {}

        @Override
        public List<TestingSplit> snapshotState(long checkpointId) {
            return java.util.Collections.emptyList();
        }

        @Override
        public void addSplits(List<TestingSplit> splits) {}

        @Override
        public void handleNoMoreSplits() {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static final class TestingEnumerator
            implements SourceSplitEnumerator<TestingSplit, Integer> {

        private final AtomicBoolean enumeratorClosed;

        private TestingEnumerator(AtomicBoolean enumeratorClosed) {
            this.enumeratorClosed = enumeratorClosed;
        }

        @Override
        public void open() {}

        @Override
        public void run() {}

        @Override
        public void close() {
            enumeratorClosed.set(true);
        }

        @Override
        public void addSplitsBack(List<TestingSplit> splits, int subtaskId) {}

        @Override
        public int currentUnassignedSplitSize() {
            return 0;
        }

        @Override
        public void handleSplitRequest(int subtaskId) {}

        @Override
        public void registerReader(int subtaskId) {}

        @Override
        public Integer snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}
    }

    private static final class TestingSplit implements SourceSplit {

        @Override
        public String splitId() {
            return "split-0";
        }
    }
}

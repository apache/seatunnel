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

package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.alibaba.fluss.client.table.scanner.log.LogScanner;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the reader's assignment-time / restore decisions:
 *
 * <ul>
 *   <li>{@code isDrainedAtAssignment} — the local, earliest-free check that a bounded split whose
 *       recorded start already reaches its end has nothing to read.
 *   <li>the {@code handleSplitsChanges} split-change-type guard.
 *   <li>the behavioral wiring that a fully consumed restore split is reported finished on the next
 *       {@code fetch()} without ever subscribing.
 * </ul>
 */
class FlussSourceSplitReaderTest {

    private static final long EARLIEST_SENTINEL = LogScanner.EARLIEST_OFFSET;
    private static final long UNBOUNDED = Long.MAX_VALUE;

    private static final TablePath TABLE_PATH = TablePath.of("fluss_db", "fluss_tbl");
    private static final long TABLE_ID = 1L;
    private static final int BUCKET_ID = 0;

    @Test
    void fullyConsumedRestoreSplitIsDrainedAtAssignment() {
        Assertions.assertTrue(FlussSourceSplitReader.isDrainedAtAssignment(100L, 100L));
        Assertions.assertTrue(FlussSourceSplitReader.isDrainedAtAssignment(150L, 100L));
    }

    @Test
    void partiallyConsumedRestoreSplitIsNotDrainedAtAssignment() {
        Assertions.assertFalse(FlussSourceSplitReader.isDrainedAtAssignment(50L, 100L));
        Assertions.assertFalse(FlussSourceSplitReader.isDrainedAtAssignment(0L, 100L));
    }

    @Test
    void freshSplitIsNotDrainedAtAssignmentUnlessEmpty() {
        Assertions.assertFalse(
                FlussSourceSplitReader.isDrainedAtAssignment(EARLIEST_SENTINEL, 100L));
        Assertions.assertTrue(FlussSourceSplitReader.isDrainedAtAssignment(EARLIEST_SENTINEL, 0L));
    }

    @Test
    void streamingSplitIsNeverDrainedAtAssignment() {
        Assertions.assertFalse(
                FlussSourceSplitReader.isDrainedAtAssignment(EARLIEST_SENTINEL, UNBOUNDED));
        Assertions.assertFalse(FlussSourceSplitReader.isDrainedAtAssignment(1000L, UNBOUNDED));
    }

    @Test
    void handleSplitsChangesRejectsNonAddition() {
        FlussSourceSplitReader reader = new FlussSourceSplitReader(newMockConfig());
        SplitsChange<FlussSourceSplit> nonAddition =
                new UnsupportedSplitsChange(
                        Collections.singletonList(
                                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 0L, 100L)));

        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> reader.handleSplitsChanges(nonAddition));
    }

    @Test
    void fullyConsumedRestoreSplitIsFinishedWithoutSubscribing() throws Exception {
        FlussSourceSplitReader reader = new FlussSourceSplitReader(newMockConfig());
        FlussSourceSplit drained =
                new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, 100L, 100L);

        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(drained)));

        RecordsWithSplitIds<FlussRecord> records = reader.fetch();
        Assertions.assertEquals(
                Collections.singleton(drained.splitId()),
                records.finishedSplits(),
                "A drained restore split must be reported finished without subscribing");
        Assertions.assertNull(
                records.nextSplit(), "No split should carry records for a drained assignment");
    }

    private static FlussSourceConfig newMockConfig() {
        FlussSourceConfig config = mock(FlussSourceConfig.class);
        when(config.getPollTimeoutMs()).thenReturn(10L);
        return config;
    }

    private static final class UnsupportedSplitsChange extends SplitsChange<FlussSourceSplit> {
        private UnsupportedSplitsChange(Collection<FlussSourceSplit> splits) {
            super(splits);
        }
    }
}

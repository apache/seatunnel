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

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.exception.FetchException;
import com.alibaba.fluss.exception.LogOffsetOutOfRangeException;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    @Test
    void emptyPollProducesNoRecordsAndNoFinishedSplits() throws Exception {
        LogScanner scanner = mock(LogScanner.class);
        ScanRecords empty = mock(ScanRecords.class);
        when(empty.buckets()).thenReturn(Collections.emptySet());
        when(scanner.poll(any(Duration.class))).thenReturn(empty);
        FlussAdminClient admin = mock(FlussAdminClient.class);

        TestReader reader = new TestReader(newMockConfig(), scanner, admin);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(0L, UNBOUNDED))));

        RecordsWithSplitIds<FlussRecord> records = reader.fetch();

        Assertions.assertNull(records.nextSplit(), "No split should carry records");
        Assertions.assertTrue(records.finishedSplits().isEmpty(), "Nothing should be finished");
        verify(scanner).subscribe(BUCKET_ID, 0L);
        verify(scanner).poll(any(Duration.class));
    }

    @Test
    void outOfRangeResetsLaggingBucketToEarliestAndDoesNotThrow() throws Exception {
        LogScanner scanner = mock(LogScanner.class);
        // A restored bucket subscribed at 100 whose offset retention discarded.
        RuntimeException outOfRange = outOfRangeFetchException();
        when(scanner.poll(any(Duration.class))).thenThrow(outOfRange);

        FlussAdminClient admin = mock(FlussAdminClient.class);
        // earliest advanced to 200, past the lagging nextOffset 100 -> recoverable truncation.
        when(admin.earliestOffsets(any(), any()))
                .thenReturn(Collections.singletonMap(BUCKET_ID, 200L));

        TestReader reader = new TestReader(newMockConfig(), scanner, admin);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(100L, UNBOUNDED))));

        RecordsWithSplitIds<FlussRecord> records = reader.fetch(); // must NOT throw

        Assertions.assertTrue(records.finishedSplits().isEmpty());
        verify(scanner).subscribe(BUCKET_ID, 100L); // initial assignment subscribe
        verify(scanner).subscribe(BUCKET_ID, 200L); // reset to earliest
    }

    @Test
    void outOfRangeWithNothingBehindEarliestRethrows() throws Exception {
        LogScanner scanner = mock(LogScanner.class);
        RuntimeException outOfRange = outOfRangeFetchException();
        when(scanner.poll(any(Duration.class))).thenThrow(outOfRange);

        FlussAdminClient admin = mock(FlussAdminClient.class);
        // earliest == nextOffset -> no bucket is behind earliest -> not a front-side
        // truncation.
        when(admin.earliestOffsets(any(), any()))
                .thenReturn(Collections.singletonMap(BUCKET_ID, 100L));

        TestReader reader = new TestReader(newMockConfig(), scanner, admin);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(100L, UNBOUNDED))));

        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, reader::fetch);
        // The original out-of-range surfaces (its cause is the
        // LogOffsetOutOfRangeException).
        Assertions.assertSame(outOfRange, thrown);
        // Only the assignment-time subscribe; no reset subscribe.
        verify(scanner).subscribe(BUCKET_ID, 100L);
    }

    @Test
    void boundedSentinelBucketDrainedByRetentionIsFinished() throws Exception {
        LogScanner scanner = mock(LogScanner.class);
        ScanRecords empty = mock(ScanRecords.class);
        when(empty.buckets()).thenReturn(Collections.emptySet());
        when(scanner.poll(any(Duration.class))).thenReturn(empty);

        FlussAdminClient admin = mock(FlussAdminClient.class);
        // earliest (60) has reached/passed the bounded end (50): the bucket is drained empty.
        when(admin.earliestOffsets(any(), any()))
                .thenReturn(Collections.singletonMap(BUCKET_ID, 60L));

        TestReader reader = new TestReader(newMockConfig(), scanner, admin);
        // Bounded split still on the -2 sentinel (never overruns, so no exception fires).
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(EARLIEST_SENTINEL, 50L))));

        RecordsWithSplitIds<FlussRecord> records = reader.fetch();

        Assertions.assertEquals(
                Collections.singleton(split(EARLIEST_SENTINEL, 50L).splitId()),
                records.finishedSplits(),
                "A bounded sentinel bucket whose earliest reached its end must be finished");
    }

    @Test
    void boundedSentinelBucketNotFinishedWhenEarliestBelowEnd() throws Exception {
        LogScanner scanner = mock(LogScanner.class);
        ScanRecords empty = mock(ScanRecords.class);
        when(empty.buckets()).thenReturn(Collections.emptySet());
        when(scanner.poll(any(Duration.class))).thenReturn(empty);

        FlussAdminClient admin = mock(FlussAdminClient.class);
        // earliest (40) is still below the bounded end (50): data may still arrive; do not finish.
        when(admin.earliestOffsets(any(), any()))
                .thenReturn(Collections.singletonMap(BUCKET_ID, 40L));

        TestReader reader = new TestReader(newMockConfig(), scanner, admin);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(split(EARLIEST_SENTINEL, 50L))));

        RecordsWithSplitIds<FlussRecord> records = reader.fetch();

        Assertions.assertTrue(
                records.finishedSplits().isEmpty(),
                "A sentinel bucket must not finish while earliest is below its end offset");
    }

    private static FlussSourceConfig newMockConfig() {
        FlussSourceConfig config = mock(FlussSourceConfig.class);
        when(config.getPollTimeoutMs()).thenReturn(10L);
        return config;
    }

    private static FlussSourceSplit split(long startOffset, long endOffset) {
        return new FlussSourceSplit(TABLE_PATH, TABLE_ID, BUCKET_ID, startOffset, endOffset);
    }

    private static RuntimeException outOfRangeFetchException() {
        LogOffsetOutOfRangeException cause = mock(LogOffsetOutOfRangeException.class);
        FetchException e = mock(FetchException.class);
        when(e.getCause()).thenReturn(cause);
        return e;
    }

    private static RowType oneIntRowType() {
        return new RowType(Collections.singletonList(new DataField("f0", DataTypes.INT())));
    }

    private static final class TestReader extends FlussSourceSplitReader {
        private final LogScanner logScanner;
        private final FlussAdminClient admin;

        TestReader(FlussSourceConfig config, LogScanner logScanner, FlussAdminClient admin) {
            super(config);
            this.logScanner = logScanner;
            this.admin = admin;
        }

        @Override
        FlussAdminClient adminClient() {
            return admin;
        }

        @Override
        TableScan createTableScan() {
            return new TableScan(TABLE_PATH, mock(Table.class), logScanner, oneIntRowType());
        }
    }

    private static final class UnsupportedSplitsChange extends SplitsChange<FlussSourceSplit> {
        private UnsupportedSplitsChange(Collection<FlussSourceSplit> splits) {
            super(splits);
        }
    }
}

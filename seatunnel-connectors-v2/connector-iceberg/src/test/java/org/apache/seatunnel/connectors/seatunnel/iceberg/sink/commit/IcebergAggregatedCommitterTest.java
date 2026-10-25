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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergAggregatedCommitterTest {

    @Mock private IcebergFilesCommitter filesCommitter;

    private IcebergAggregatedCommitter committer;

    @BeforeEach
    void setUp() throws Exception {
        committer = new IcebergAggregatedCommitter(null, null);
        Field field = IcebergAggregatedCommitter.class.getDeclaredField("filesCommitter");
        field.setAccessible(true);
        field.set(committer, filesCommitter);
    }

    @Test
    void testCommitMergesAllWorkersIntoSingleSnapshot() throws Exception {
        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r1 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r2 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);

        IcebergCommitInfo worker0 = new IcebergCommitInfo(Collections.singletonList(r0), 1L);
        IcebergCommitInfo worker1 = new IcebergCommitInfo(Collections.singletonList(r1), 1L);
        IcebergCommitInfo worker2 = new IcebergCommitInfo(Collections.singletonList(r2), 1L);

        IcebergAggregatedCommitInfo aggregated =
                committer.combine(Arrays.asList(worker0, worker1, worker2));
        committer.commit(Collections.singletonList(aggregated));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteResult>> captor = ArgumentCaptor.forClass(List.class);
        verify(filesCommitter).doCommit(captor.capture(), eq(1L));

        List<WriteResult> committed = captor.getValue();
        assertEquals(3, committed.size());
        assertEquals(r0, committed.get(0));
        assertEquals(r1, committed.get(1));
        assertEquals(r2, committed.get(2));
    }

    @Test
    void testCommitSkipsWhenAllResultsEmpty() throws Exception {
        IcebergCommitInfo empty0 = new IcebergCommitInfo(Collections.emptyList(), 2L);
        IcebergCommitInfo empty1 = new IcebergCommitInfo(null, 2L);

        IcebergAggregatedCommitInfo aggregated = committer.combine(Arrays.asList(empty0, empty1));
        committer.commit(Collections.singletonList(aggregated));

        verify(filesCommitter, never()).doCommit(any(), anyLong());
    }

    @Test
    void testCommitMultipleCheckpointsEachProduceExactlyOneSnapshot() throws Exception {
        WriteResult ckpt1r0 =
                new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult ckpt1r1 =
                new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult ckpt2r0 =
                new WriteResult(Collections.emptyList(), Collections.emptyList(), null);

        IcebergAggregatedCommitInfo checkpoint1 =
                committer.combine(
                        Arrays.asList(
                                new IcebergCommitInfo(Collections.singletonList(ckpt1r0), 1L),
                                new IcebergCommitInfo(Collections.singletonList(ckpt1r1), 1L)));
        IcebergAggregatedCommitInfo checkpoint2 =
                committer.combine(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(ckpt2r0), 2L)));

        committer.commit(Arrays.asList(checkpoint1, checkpoint2));

        verify(filesCommitter, times(2)).doCommit(any(), anyLong());
    }

    @Test
    void testCommitPropagatesExceptionAndDoesNotRetry() throws Exception {
        doThrow(new RuntimeException("simulated Iceberg commit failure"))
                .when(filesCommitter)
                .doCommit(any(), anyLong());

        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r1 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);

        IcebergAggregatedCommitInfo aggregated =
                committer.combine(
                        Arrays.asList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 1L),
                                new IcebergCommitInfo(Collections.singletonList(r1), 1L)));

        assertThrows(
                RuntimeException.class,
                () -> committer.commit(Collections.singletonList(aggregated)),
                "commit() must propagate the Iceberg exception to the engine");

        verify(filesCommitter, times(1)).doCommit(any(), eq(1L));
    }

    @Test
    void testRestoreCommitSkipsCheckpointAlreadyCommittedToIceberg() throws Exception {
        when(filesCommitter.isAlreadyCommitted(eq(5L), any())).thenReturn(true);

        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        IcebergAggregatedCommitInfo aggregated =
                committer.combine(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 5L)));

        committer.restoreCommit(Collections.singletonList(aggregated));

        verify(filesCommitter, never()).doCommit(any(), anyLong());
    }

    @Test
    void testRestoreCommitRecommitsCheckpointNotYetInIceberg() throws Exception {
        when(filesCommitter.isAlreadyCommitted(eq(5L), any())).thenReturn(false);

        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        IcebergAggregatedCommitInfo aggregated =
                committer.combine(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 5L)));

        committer.restoreCommit(Collections.singletonList(aggregated));

        verify(filesCommitter, times(1)).doCommit(any(), eq(5L));
    }

    @Test
    void testRestoreCommitWithUnknownCheckpointIdCommitsWhenFilesNotYetPresent() throws Exception {
        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        IcebergAggregatedCommitInfo aggregated =
                new IcebergAggregatedCommitInfo(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 0L)));

        when(filesCommitter.isAlreadyCommitted(eq(0L), any())).thenReturn(false);

        committer.restoreCommit(Collections.singletonList(aggregated));

        verify(filesCommitter, times(1)).isAlreadyCommitted(eq(0L), any());
        verify(filesCommitter, times(1)).doCommit(any(), eq(0L));
    }

    @Test
    void testRestoreCommitWithUnknownCheckpointIdSkipsWhenFilesAlreadyPresent() throws Exception {
        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        IcebergAggregatedCommitInfo aggregated =
                new IcebergAggregatedCommitInfo(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 0L)));

        when(filesCommitter.isAlreadyCommitted(eq(0L), any())).thenReturn(true);

        committer.restoreCommit(Collections.singletonList(aggregated));

        verify(filesCommitter, times(1)).isAlreadyCommitted(eq(0L), any());
        verify(filesCommitter, never()).doCommit(any(), anyLong());
    }

    @Test
    void testRestoreCommitSkipsWhenAllResultsEmpty() throws Exception {
        IcebergCommitInfo empty0 = new IcebergCommitInfo(Collections.emptyList(), 6L);
        IcebergCommitInfo empty1 = new IcebergCommitInfo(null, 6L);

        IcebergAggregatedCommitInfo aggregated = committer.combine(Arrays.asList(empty0, empty1));
        committer.restoreCommit(Collections.singletonList(aggregated));

        verify(filesCommitter, never()).doCommit(any(), anyLong());
    }

    @Test
    void testRestoreCommitMergesAllWorkersIntoSingleSnapshot() throws Exception {
        when(filesCommitter.isAlreadyCommitted(eq(3L), any())).thenReturn(false);

        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r1 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r2 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);

        IcebergAggregatedCommitInfo aggregated =
                committer.combine(
                        Arrays.asList(
                                new IcebergCommitInfo(Collections.singletonList(r0), 3L),
                                new IcebergCommitInfo(Collections.singletonList(r1), 3L),
                                new IcebergCommitInfo(Collections.singletonList(r2), 3L)));

        committer.restoreCommit(Collections.singletonList(aggregated));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteResult>> captor = ArgumentCaptor.forClass(List.class);
        verify(filesCommitter, times(1)).doCommit(captor.capture(), eq(3L));
        assertEquals(3, captor.getValue().size());
    }
}

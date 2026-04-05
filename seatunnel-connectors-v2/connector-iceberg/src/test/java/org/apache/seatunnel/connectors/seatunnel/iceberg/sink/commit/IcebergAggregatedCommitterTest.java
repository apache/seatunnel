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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

        IcebergCommitInfo worker0 = new IcebergCommitInfo(Collections.singletonList(r0));
        IcebergCommitInfo worker1 = new IcebergCommitInfo(Collections.singletonList(r1));
        IcebergCommitInfo worker2 = new IcebergCommitInfo(Collections.singletonList(r2));

        IcebergAggregatedCommitInfo aggregated =
                committer.combine(Arrays.asList(worker0, worker1, worker2));
        committer.commit(Collections.singletonList(aggregated));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteResult>> captor = ArgumentCaptor.forClass(List.class);
        verify(filesCommitter).doCommit(captor.capture());

        List<WriteResult> committed = captor.getValue();
        assertEquals(3, committed.size());
        assertEquals(r0, committed.get(0));
        assertEquals(r1, committed.get(1));
        assertEquals(r2, committed.get(2));
    }

    @Test
    void testCommitSkipsWhenAllResultsEmpty() throws Exception {
        IcebergCommitInfo empty0 = new IcebergCommitInfo(Collections.emptyList());
        IcebergCommitInfo empty1 = new IcebergCommitInfo(null);

        IcebergAggregatedCommitInfo aggregated = committer.combine(Arrays.asList(empty0, empty1));
        committer.commit(Collections.singletonList(aggregated));

        verify(filesCommitter, never()).doCommit(any());
    }

    @Test
    void testCommitSkipsEmptyWorkersAndMergesRest() throws Exception {
        WriteResult r0 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);
        WriteResult r1 = new WriteResult(Collections.emptyList(), Collections.emptyList(), null);

        IcebergCommitInfo activeWorker0 = new IcebergCommitInfo(Collections.singletonList(r0));
        IcebergCommitInfo activeWorker1 = new IcebergCommitInfo(Collections.singletonList(r1));
        IcebergCommitInfo emptyWorker = new IcebergCommitInfo(Collections.emptyList());
        IcebergCommitInfo nullWorker = new IcebergCommitInfo(null);

        IcebergAggregatedCommitInfo aggregated =
                committer.combine(
                        Arrays.asList(activeWorker0, emptyWorker, nullWorker, activeWorker1));
        committer.commit(Collections.singletonList(aggregated));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriteResult>> captor = ArgumentCaptor.forClass(List.class);
        verify(filesCommitter).doCommit(captor.capture());

        List<WriteResult> committed = captor.getValue();
        assertEquals(2, committed.size());
        assertEquals(r0, committed.get(0));
        assertEquals(r1, committed.get(1));
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
                                new IcebergCommitInfo(Collections.singletonList(ckpt1r0)),
                                new IcebergCommitInfo(Collections.singletonList(ckpt1r1))));
        IcebergAggregatedCommitInfo checkpoint2 =
                committer.combine(
                        Collections.singletonList(
                                new IcebergCommitInfo(Collections.singletonList(ckpt2r0))));

        committer.commit(Arrays.asList(checkpoint1, checkpoint2));

        verify(filesCommitter, times(2)).doCommit(any());
    }
}

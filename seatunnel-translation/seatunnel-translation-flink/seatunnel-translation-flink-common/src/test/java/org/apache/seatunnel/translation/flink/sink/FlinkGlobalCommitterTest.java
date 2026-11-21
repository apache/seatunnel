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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlinkGlobalCommitterTest {
    @Test
    void testRecoveredTransactionsAreReconciledInsteadOfDiscarded() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        List<String> restored = Collections.singletonList("prepared");
        List<String> pending = Collections.singletonList("retry");
        when(delegate.restoreCommit(restored)).thenReturn(pending);
        FlinkGlobalCommitter<String, String> committer = new FlinkGlobalCommitter<>(delegate);
        IOException failure =
                Assertions.assertThrows(
                        IOException.class, () -> committer.filterRecoveredCommittables(restored));
        Assertions.assertTrue(
                failure.getMessage().contains("1 unresolved transactions (restoreCommit)"));
        verify(delegate).restoreCommit(restored);
        committer.close();
    }

    @Test
    void testLiveCommitFailsWithoutAcknowledgingPendingRetries() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        List<String> batch = Collections.singletonList("prepared");
        List<String> pending = Collections.singletonList("retry");
        when(delegate.commit(batch)).thenReturn(pending);
        FlinkGlobalCommitter<String, String> committer = new FlinkGlobalCommitter<>(delegate);
        IOException failure =
                Assertions.assertThrows(IOException.class, () -> committer.commit(batch));
        Assertions.assertTrue(failure.getMessage().contains("restoreCommit=0, commit=1"));
        committer.close();
    }

    @Test
    void testRestoredRegularCommittableAndRecoveryRetry() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        RecoveredCommitTracker tracker = new RecoveredCommitTracker();
        FlinkGlobalCommitter<String, String> committer =
                new FlinkGlobalCommitter<>(delegate, tracker);
        CommitWrapper<String> wrapper = new CommitWrapper<>("prepared");
        tracker.deserialized(wrapper);
        committer.filterRecoveredCommittables(Collections.emptyList());
        when(delegate.combine(Collections.singletonList("prepared"))).thenReturn("combined");
        String combined = committer.combine(Collections.singletonList(wrapper));
        when(delegate.restoreCommit(Collections.singletonList(combined)))
                .thenReturn(Collections.singletonList("retry"));
        Assertions.assertThrows(
                IOException.class, () -> committer.commit(Collections.singletonList(combined)));
        committer.commit(Collections.singletonList("retry"));
        verify(delegate).restoreCommit(Collections.singletonList("retry"));
        Mockito.verify(delegate, Mockito.never()).commit(ArgumentMatchers.anyList());
        committer.close();
    }

    @Test
    void testFreshTrafficAfterRecoveryRemainsStrict() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        RecoveredCommitTracker tracker = new RecoveredCommitTracker();
        FlinkGlobalCommitter<String, String> committer =
                new FlinkGlobalCommitter<>(delegate, tracker);
        committer.filterRecoveredCommittables(Collections.emptyList());
        CommitWrapper<String> live = new CommitWrapper<>("prepared");
        tracker.deserialized(live);
        Assertions.assertFalse(live.isRecovered());
        when(delegate.combine(Collections.singletonList("prepared"))).thenReturn("combined");
        String combined = committer.combine(Collections.singletonList(live));
        Mockito.doThrow(new IOException("unknown live transaction"))
                .when(delegate)
                .commit(Collections.singletonList(combined));
        Assertions.assertThrows(
                IOException.class, () -> committer.commit(Collections.singletonList(combined)));
        committer.close();
    }

    @Test
    void testMixedAggregateUsesRecoveryAndReusedLiveAggregateFailsClosed() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        FlinkGlobalCommitter<String, String> committer = new FlinkGlobalCommitter<>(delegate);
        CommitWrapper<String> restored = new CommitWrapper<>("restored");
        restored.markRecovered();
        CommitWrapper<String> live = new CommitWrapper<>("live");
        when(delegate.combine(Arrays.asList("restored", "live"))).thenReturn("mixed");
        String mixed = committer.combine(Arrays.asList(restored, live));
        committer.commit(Collections.singletonList(mixed));
        verify(delegate).restoreCommit(Collections.singletonList("mixed"));
        String reused = new String("combined");
        when(delegate.combine(ArgumentMatchers.anyList())).thenReturn(reused);
        committer.combine(Collections.singletonList(restored));
        Assertions.assertThrows(
                IOException.class, () -> committer.combine(Collections.singletonList(live)));
        committer.close();
    }

    @Test
    void testRecoveryRemainsMarkedWhenLaterLiveCommitThrows() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        FlinkGlobalCommitter<String, String> committer = new FlinkGlobalCommitter<>(delegate);
        CommitWrapper<String> replay = new CommitWrapper<>("old");
        replay.markRecovered();
        when(delegate.combine(Collections.singletonList("old"))).thenReturn("old-combined");
        String old = committer.combine(Collections.singletonList(replay));
        Mockito.doThrow(new IOException("live failure"))
                .when(delegate)
                .commit(Collections.singletonList("live"));
        Assertions.assertThrows(
                IOException.class, () -> committer.commit(Arrays.asList(old, "live")));
        committer.commit(Collections.singletonList(old));
        verify(delegate, Mockito.times(2)).restoreCommit(Collections.singletonList(old));
        committer.close();
    }

    @Test
    void testLegacyRecoveryRetryRemainsRecoveryAndEqualLiveValueIsStrict() throws Exception {
        SinkAggregatedCommitter<String, String> delegate = mock(SinkAggregatedCommitter.class);
        FlinkGlobalCommitter<String, String> committer = new FlinkGlobalCommitter<>(delegate);
        String retry = new String("retry");
        when(delegate.restoreCommit(Collections.singletonList("saved")))
                .thenReturn(Collections.singletonList(retry));
        Assertions.assertThrows(
                IOException.class,
                () -> committer.filterRecoveredCommittables(Collections.singletonList("saved")));
        String equalLive = new String("retry");
        committer.commit(Arrays.asList(retry, equalLive));
        verify(delegate).restoreCommit(Collections.singletonList(retry));
        verify(delegate).commit(Collections.singletonList(equalLive));
        committer.close();
    }
}

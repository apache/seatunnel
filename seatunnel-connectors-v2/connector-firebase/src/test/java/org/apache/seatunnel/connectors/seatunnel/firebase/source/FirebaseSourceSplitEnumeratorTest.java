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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.firebase.client.FirebaseHttpClient;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FirebaseSourceSplitEnumeratorTest {
    private SourceSplitEnumerator.Context<FirebaseSourceSplit> mockContext;
    private FirebaseHttpClient mockHttpClient;
    private ReadonlyConfig config;

    @BeforeEach
    void setUp() {
        mockContext = mock(SourceSplitEnumerator.Context.class);
        mockHttpClient = mock(FirebaseHttpClient.class);
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        config = ReadonlyConfig.fromMap(configMap);

        when(mockContext.currentParallelism()).thenReturn(2);
    }

    @Test
    void testAssignSplitsFromRestoredState() throws Exception {
        // 1. Mock registered reader subtask [0]
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);
        when(mockContext.currentParallelism()).thenReturn(1);

        // 2. Build ReadonlyConfig
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(
                FirebaseSourceOptions.URL.key(), "https://test-50a28-default-rtdb.firebaseio.com");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // 3. Build state with pre-created split
        FirebaseSourceSplit split =
                new FirebaseSourceSplit(
                        "split_key_range_0_0", "users", Collections.singletonList("user_101"));
        FirebaseSourceState state =
                new FirebaseSourceState(Collections.singleton(split), new HashSet<>());

        // 4. Instantiate enumerator with restored state
        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, state);

        // 5. Execute run() and registerReader()
        enumerator.run();
        enumerator.registerReader(0);

        // 6. Assert interactions on mockContext (using batch assignSplits)
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), Collections.singletonList(any()));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testRunKeyDiscoveryAndAssignSplits() throws Exception {
        // 1. Mock registered reader subtask [0]
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);
        when(mockContext.currentParallelism()).thenReturn(2);

        // 2. Mock HTTP client key response
        when(mockHttpClient.fetchShallowKeys()).thenReturn(Arrays.asList("user_101", "user_102"));

        // 3. Build ReadonlyConfig
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        configMap.put(FirebaseSourceOptions.URL.key(), "https://test-db.firebaseio.com");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // 4. Instantiate enumerator with mocked HTTP client
        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, mockHttpClient);

        // 5. Register reader subtask 0
        enumerator.registerReader(0);

        // 6. Execute run() — triggers shallow scan + partitioning + assignSplits()
        enumerator.run();

        // 7. Verify splits were assigned to reader 0 and NO_MORE_SPLITS was signaled
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), Collections.singletonList(any()));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testFallbackToSinglePathSplitWhenKeysEmpty() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);
        when(mockContext.currentParallelism()).thenReturn(1);

        // Simulate empty keys or exception fallback
        when(mockHttpClient.fetchShallowKeys()).thenReturn(Collections.emptyList());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, mockHttpClient);

        enumerator.registerReader(0);
        enumerator.run();

        // Verify single path fallback split assignment
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), Collections.singletonList(any()));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testAddSplitsBackReQueuesAndReAssigns() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FirebaseSourceOptions.PATH.key(), "users");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, mockHttpClient);

        FirebaseSourceSplit split =
                new FirebaseSourceSplit(
                        "split_failed_1", "users", Collections.singletonList("user_101"));

        // Simulate subtask failover returning split back
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        // Verify returned split gets re-assigned immediately to available reader subtask 0
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<FirebaseSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), captor.capture());

        List<FirebaseSourceSplit> assignedSplits = captor.getValue();
        assertEquals(1, assignedSplits.size());
        assertEquals("split_failed_1", assignedSplits.get(0).splitId());
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    public void testRestorePathSignalsNoMoreSplitsToLateRegisteringReaders() throws Exception {
        // 1. Mock Context
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<FirebaseSourceSplit> mockContext =
                mock(SourceSplitEnumerator.Context.class);

        // 2. Prepare restored state with 1 pending split
        FirebaseSourceSplit restoredSplit = new FirebaseSourceSplit("split_restored_1", "users");
        Set<FirebaseSourceSplit> pendingSplits =
                new HashSet<>(Collections.singletonList(restoredSplit));
        Set<String> assignedSplitIds = new HashSet<>();
        FirebaseSourceState restoredState =
                new FirebaseSourceState(pendingSplits, assignedSplitIds);

        // 3. Construct ReadonlyConfig with both "url" and "path"
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "https://test-db.firebaseio.com");
        configMap.put("path", "users");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // 4. Construct enumerator from state
        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, restoredState);

        // 5. Step 1: Reader 0 registers first
        Set<Integer> registeredReaders = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(registeredReaders);

        enumerator.registerReader(0);

        // Verify Reader 0 received the split and NO_MORE_SPLITS signal
        ArgumentCaptor<List<FirebaseSourceSplit>> splitCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockContext).assignSplit(eq(0), splitCaptor.capture());
        assertEquals(1, splitCaptor.getValue().size());
        assertEquals("split_restored_1", splitCaptor.getValue().get(0).splitId());
        verify(mockContext).signalNoMoreSplits(eq(0));

        // 6. Step 2: Reader 1 registers later (pendingSplits is now empty)
        registeredReaders.add(1); // registeredReaders is now {0, 1}
        when(mockContext.registeredReaders()).thenReturn(registeredReaders);

        enumerator.registerReader(1);

        // 7. Assertion: Reader 1 MUST receive NO_MORE_SPLITS signal despite pendingSplits being
        // empty
        verify(mockContext).signalNoMoreSplits(eq(1));
    }

    @Test
    void testConcurrentStateAccessDoesNotThrowException() throws Exception {
        FirebaseSourceSplitEnumerator enumerator =
                new FirebaseSourceSplitEnumerator(mockContext, config, mockHttpClient);

        int iterations = 1000;
        int numThreads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<?>> futures = new ArrayList<>();

        // Thread 1: Simulates checkpointing thread calling snapshotState()
        futures.add(
                executor.submit(
                        () -> {
                            startLatch.await();
                            for (int i = 0; i < iterations; i++) {
                                FirebaseSourceState state = enumerator.snapshotState(i);
                                assertNotNull(state);
                            }
                            return null;
                        }));

        // Thread 2: Simulates failover/retry thread calling addSplitsBack()
        futures.add(
                executor.submit(
                        () -> {
                            startLatch.await();
                            for (int i = 0; i < iterations; i++) {
                                FirebaseSourceSplit split =
                                        new FirebaseSourceSplit("split_concurrent_" + i, "users");
                                enumerator.addSplitsBack(Collections.singletonList(split), 0);
                            }
                            return null;
                        }));

        // Thread 3: Simulates Hazelcast operation thread registering readers & requesting splits
        futures.add(
                executor.submit(
                        () -> {
                            startLatch.await();
                            for (int i = 0; i < iterations; i++) {
                                int subtaskId = i % 2;
                                enumerator.registerReader(subtaskId);
                                enumerator.handleSplitRequest(subtaskId);
                            }
                            return null;
                        }));

        // Release all threads simultaneously to create maximum contention
        startLatch.countDown();

        // Verify that none of the threads threw a ConcurrentModificationException
        for (Future<?> future : futures) {
            assertDoesNotThrow(
                    () -> future.get(10, TimeUnit.SECONDS),
                    "Thread execution threw an exception due to a race condition or lock missing");
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
}

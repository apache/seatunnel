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
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FirebaseSourceSplitEnumeratorTest {
    private SourceSplitEnumerator.Context<FirebaseSourceSplit> mockContext;
    private FirebaseHttpClient mockHttpClient;

    @BeforeEach
    void setUp() {
        mockContext = Mockito.mock(SourceSplitEnumerator.Context.class);
        mockHttpClient = Mockito.mock(FirebaseHttpClient.class);
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

        // 6. Assert interactions on mockContext
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), eq(split));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
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
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), any(FirebaseSourceSplit.class));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
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
        verify(mockContext, atLeastOnce()).assignSplit(eq(0), any(FirebaseSourceSplit.class));
        verify(mockContext, atLeastOnce()).signalNoMoreSplits(0);
    }
}

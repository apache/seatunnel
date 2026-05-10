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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class FlinkSourceEnumeratorTest {

    private static final class DummySplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    @Test
    void testResignalNoMoreSplitsAfterReaderReregister() {
        SourceSplitEnumerator<DummySplit, Serializable> sourceSplitEnumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumeratorContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(2);

        Set<Integer> noMoreSplitsSignaledReaders = ConcurrentHashMap.newKeySet();
        noMoreSplitsSignaledReaders.add(0);

        FlinkSourceEnumerator<DummySplit, Serializable> enumerator =
                new FlinkSourceEnumerator<>(
                        sourceSplitEnumerator, enumeratorContext, noMoreSplitsSignaledReaders);

        enumerator.addReader(0);

        Mockito.verify(enumeratorContext).signalNoMoreSplits(0);
    }

    @Test
    void testDuplicateReaderRegistrationDoesNotStartEnumeratorEarly() throws Exception {
        SourceSplitEnumerator<DummySplit, Serializable> sourceSplitEnumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumeratorContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(2);

        FlinkSourceEnumerator<DummySplit, Serializable> enumerator =
                new FlinkSourceEnumerator<>(
                        sourceSplitEnumerator, enumeratorContext, ConcurrentHashMap.newKeySet());

        enumerator.addReader(0);
        enumerator.addReader(0);

        Mockito.verify(sourceSplitEnumerator, Mockito.never()).run();

        enumerator.addReader(1);

        Mockito.verify(sourceSplitEnumerator).run();
    }

    @Test
    void testRunFailureCanRetryOnReaderReregister() throws Exception {
        SourceSplitEnumerator<DummySplit, Serializable> sourceSplitEnumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumeratorContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(1);
        Mockito.doThrow(new RuntimeException("run failed"))
                .doNothing()
                .when(sourceSplitEnumerator)
                .run();

        FlinkSourceEnumerator<DummySplit, Serializable> enumerator =
                new FlinkSourceEnumerator<>(
                        sourceSplitEnumerator, enumeratorContext, ConcurrentHashMap.newKeySet());

        Assertions.assertThrows(RuntimeException.class, () -> enumerator.addReader(0));

        enumerator.addReader(0);

        Mockito.verify(sourceSplitEnumerator, Mockito.times(2)).run();
    }

    @Test
    void testSnapshotStateDoesNotWaitForBlockingRun() throws Exception {
        SourceSplitEnumerator<DummySplit, Serializable> sourceSplitEnumerator =
                Mockito.mock(SourceSplitEnumerator.class);
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumeratorContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(1);
        Mockito.when(sourceSplitEnumerator.snapshotState(1L)).thenReturn("checkpoint");

        CountDownLatch runEntered = new CountDownLatch(1);
        CountDownLatch releaseRun = new CountDownLatch(1);
        Mockito.doAnswer(
                        invocation -> {
                            runEntered.countDown();
                            releaseRun.await(5, TimeUnit.SECONDS);
                            return null;
                        })
                .when(sourceSplitEnumerator)
                .run();

        FlinkSourceEnumerator<DummySplit, Serializable> enumerator =
                new FlinkSourceEnumerator<>(
                        sourceSplitEnumerator, enumeratorContext, ConcurrentHashMap.newKeySet());

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> addReaderFuture = executorService.submit(() -> enumerator.addReader(0));
        try {
            Assertions.assertTrue(runEntered.await(1, TimeUnit.SECONDS));

            Future<Serializable> snapshotFuture =
                    executorService.submit(() -> enumerator.snapshotState(1L));

            Assertions.assertEquals("checkpoint", snapshotFuture.get(1, TimeUnit.SECONDS));
        } finally {
            releaseRun.countDown();
            addReaderFuture.get(1, TimeUnit.SECONDS);
            executorService.shutdownNow();
        }
    }
}

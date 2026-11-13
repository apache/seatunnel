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

package org.apache.seatunnel.connectors.seatunnel.common.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Drives the enumerator through its context in the orders the engines use: on Flink, a failed
 * reader is removed, the splits assigned since the last completed checkpoint are handed back
 * (possibly none), and a new reader registers; on Zeta, a restored reader hands its split back to a
 * new enumerator before it registers, and {@code run()} follows once every reader has registered.
 */
public class SingleSplitEnumeratorTest {

    private static final int READER = 0;
    private static final int OTHER_READER = 1;

    private final Set<Integer> readers = new ConcurrentSkipListSet<>();
    private final List<Integer> assignedReaders = new CopyOnWriteArrayList<>();
    private final List<SingleSplit> assignedSplits = new CopyOnWriteArrayList<>();
    private volatile Runnable duringFirstAssignment = () -> {};
    private SingleSplitEnumerator enumerator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        SourceSplitEnumerator.Context<SingleSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.registeredReaders()).thenAnswer(invocation -> new TreeSet<>(readers));
        Mockito.doAnswer(
                        invocation -> {
                            if (assignedSplits.isEmpty()) {
                                duringFirstAssignment.run();
                            }
                            assignedReaders.add(invocation.getArgument(0));
                            assignedSplits.add(invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .assignSplit(Mockito.anyInt(), Mockito.any(SingleSplit.class));
        enumerator = new SingleSplitEnumerator(context);
    }

    @Test
    public void testFirstAssignmentWhenTheReaderRegistersBeforeRun() throws Exception {
        registerReader(READER);
        enumerator.run();
        assertAssignments(READER);

        enumerator.run();
        registerReader(READER);
        assertAssignments(READER);
    }

    @Test
    public void testFirstAssignmentWhenRunBeforeTheReaderRegisters() throws Exception {
        enumerator.run();
        assertAssignments();

        registerReader(READER);
        assertAssignments(READER);

        enumerator.run();
        registerReader(READER);
        assertAssignments(READER);
    }

    @Test
    public void testSplitGivenBackIsAssignedAgainWhenTheReaderRegisters() throws Exception {
        start();
        SingleSplit split = assignedSplits.get(0);

        readers.remove(READER);
        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        assertAssignments(READER);

        registerReader(READER);
        enumerator.run();
        assertAssignments(READER, READER);
        Assertions.assertSame(split, assignedSplits.get(1));
    }

    @Test
    public void testSplitGivenBackWhileTheReaderIsRegisteredIsAssignedAtOnce() throws Exception {
        start();
        SingleSplit split = assignedSplits.get(0);

        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        assertAssignments(READER, READER);
        Assertions.assertSame(split, assignedSplits.get(1));

        enumerator.run();
        registerReader(READER);
        assertAssignments(READER, READER);
    }

    @Test
    public void testSplitGivenBackGoesToARegisteredReader() throws Exception {
        registerReader(READER);
        registerReader(OTHER_READER);
        enumerator.run();
        assertAssignments(READER);
        SingleSplit split = assignedSplits.get(0);

        readers.remove(READER);
        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        assertAssignments(READER, OTHER_READER);
        Assertions.assertSame(split, assignedSplits.get(1));
    }

    @Test
    public void testRestoredSplitIsAssignedOnceWhenTheReaderRegistersAndRunFollows()
            throws Exception {
        SingleSplit restored = new SingleSplit(new byte[] {1});

        enumerator.addSplitsBack(Collections.singletonList(restored), READER);
        assertAssignments();

        registerReader(READER);
        enumerator.run();
        assertAssignments(READER);
        Assertions.assertSame(restored, assignedSplits.get(0));
    }

    @Test
    public void testEmptyListGivenBackIsHarmless() throws Exception {
        start();

        // The reader fails before a checkpoint completes: it gets the split back.
        restartReader(Collections.singletonList(assignedSplits.get(0)));
        assertAssignments(READER, READER);

        // It fails again: nothing was assigned since the last checkpoint, so the list is empty.
        Assertions.assertDoesNotThrow(() -> restartReader(Collections.emptyList()));
        Assertions.assertDoesNotThrow(
                () -> enumerator.addSplitsBack(Collections.emptyList(), READER));
        assertAssignments(READER, READER);
    }

    @Test
    public void testEmptyListGivenBackKeepsAPendingSplit() throws Exception {
        start();
        SingleSplit split = assignedSplits.get(0);

        // The split comes back while no reader is registered, then an empty list follows.
        readers.remove(READER);
        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        enumerator.addSplitsBack(Collections.emptyList(), READER);
        assertAssignments(READER);

        registerReader(READER);
        enumerator.run();
        assertAssignments(READER, READER);
        Assertions.assertSame(split, assignedSplits.get(1));
    }

    /**
     * On Zeta, a split given back and a reader registration reach the enumerator under different
     * engine locks. While the split is being assigned, another reader registers from a second
     * thread; it must wait and then find the split assigned, so the split is assigned once.
     */
    @Test
    public void testReaderRegisteringDuringAnAssignmentDoesNotAssignTheSplitAgain()
            throws Exception {
        readers.add(READER);
        readers.add(OTHER_READER);
        CountDownLatch registered = new CountDownLatch(1);
        List<Throwable> failures = new ArrayList<>();
        Thread otherReader =
                new Thread(
                        () -> {
                            try {
                                enumerator.registerReader(OTHER_READER);
                            } catch (Throwable t) {
                                failures.add(t);
                            } finally {
                                registered.countDown();
                            }
                        });
        duringFirstAssignment =
                () -> {
                    otherReader.start();
                    try {
                        // Without synchronization the registration completes in this window.
                        registered.await(300, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

        enumerator.addSplitsBack(Collections.singletonList(new SingleSplit(null)), READER);
        otherReader.join(TimeUnit.SECONDS.toMillis(10));

        Assertions.assertFalse(otherReader.isAlive());
        Assertions.assertEquals(Collections.emptyList(), failures);
        assertAssignments(READER);
    }

    private void start() throws Exception {
        registerReader(READER);
        enumerator.run();
        assertAssignments(READER);
    }

    private void registerReader(int reader) {
        readers.add(reader);
        enumerator.registerReader(reader);
    }

    private void restartReader(List<SingleSplit> splitsGivenBack) {
        readers.remove(READER);
        enumerator.addSplitsBack(splitsGivenBack, READER);
        registerReader(READER);
    }

    private void assertAssignments(Integer... expectedReaders) {
        Assertions.assertEquals(expectedReaders.length, assignedSplits.size(), "assignments");
        Assertions.assertEquals(Arrays.asList(expectedReaders), assignedReaders, "readers");
    }
}

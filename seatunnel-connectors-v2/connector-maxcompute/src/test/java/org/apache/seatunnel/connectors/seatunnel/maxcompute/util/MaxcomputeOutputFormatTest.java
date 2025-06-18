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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import com.aliyun.odps.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MaxcomputeOutputFormatTest {
    public static FormatterContext defaultFormatterContext =
            new FormatterContext("yyyy-MM-dd HH:mm:ss");

    private MaxcomputeOutputFormat outputFormat;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id1", "id2", "name"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        TableSchema tableSchema = mock(TableSchema.class);
        ReadonlyConfig config = mock(ReadonlyConfig.class);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("id1");
        columnNames.add("id2");
        PrimaryKey pk = new PrimaryKey("dummy", columnNames);

        when(config.get(any())).thenReturn(1);

        outputFormat =
                new MaxcomputeOutputFormat(
                        rowType, config, tableSchema, defaultFormatterContext, pk, 128);
    }

    @Test
    void testValidateLockCountWithinRange() {
        int min = outputFormat.validateLockCount(5);
        int max = outputFormat.validateLockCount(5000);
        int normal = outputFormat.validateLockCount(512);

        assertEquals(16, min);
        assertEquals(2048, max);
        assertEquals(512, normal);
    }

    @Test
    void testBuildPrimaryKey() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, 1, "A"});
        int primaryKey = outputFormat.buildPrimaryKey(row);
        int hashKey =
                31 * (31 + Integer.hashCode((int) row.getField(0)))
                        + Integer.hashCode((int) row.getField(1));
        assertEquals(hashKey, primaryKey);
    }

    @Test
    void testBuildPrimaryKeyThrowsWhenNull() {
        SeaTunnelRow row = mock(SeaTunnelRow.class);
        when(row.getFields()).thenReturn(new Object[] {null});
        when(row.getField(0)).thenReturn(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    outputFormat.buildPrimaryKey(row);
                });
    }

    @Test
    void testSameIdsUseSameLock() {
        SeaTunnelRow rowA = new SeaTunnelRow(new Object[] {1, 1, "A1"});
        SeaTunnelRow rowB = new SeaTunnelRow(new Object[] {1, 1, "A2"});

        Lock lockA = outputFormat.getLockByPrimaryKey(rowA);
        Lock lockB = outputFormat.getLockByPrimaryKey(rowB);

        assertSame(lockA, lockB, "Same rows must have same lock");
    }

    @Test
    void testDifferentIdsUseDifferentLocks() {
        SeaTunnelRow rowA = new SeaTunnelRow(new Object[] {1, 1, "A"});
        SeaTunnelRow rowB = new SeaTunnelRow(new Object[] {1, 2, "B"});

        Lock lockA = outputFormat.getLockByPrimaryKey(rowA);
        Lock lockB = outputFormat.getLockByPrimaryKey(rowB);

        assertNotSame(lockA, lockB, "Different rows must have different locks");
    }

    @Test
    void testUpsertLockProcess() throws Exception {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, 1, "A"});
        Lock lock = mock(Lock.class);
        MaxcomputeOutputFormat.CheckedRunnable runnable =
                mock(MaxcomputeOutputFormat.CheckedRunnable.class);

        MaxcomputeOutputFormat spy = spy(outputFormat);
        doReturn(lock).when(spy).getLockByPrimaryKey(row);

        spy.lockProcess(row, runnable);

        verify(lock).lock();
        verify(lock).unlock();
        verify(runnable).run();
    }

    @Test
    void testLockProcessWithSameId_MultiThreaded() throws Exception {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, 1, "A"});

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch ready = new CountDownLatch(numThreads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numThreads);

        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(
                    () -> {
                        try {
                            ready.countDown();
                            start.await();

                            outputFormat.lockProcess(
                                    row,
                                    () -> {
                                        counter.incrementAndGet();
                                    });

                        } catch (Exception e) {
                            fail(e);
                        } finally {
                            done.countDown();
                        }
                    });
        }

        ready.await();
        start.countDown();
        done.await();
        executor.shutdown();

        assertEquals(numThreads, counter.get());
    }

    @Test
    void testCountOfLockProcessWithSameId_MultiThreaded() throws Exception {
        SeaTunnelRow row = mock(SeaTunnelRow.class);

        Lock lock = mock(Lock.class);
        MaxcomputeOutputFormat spy = spy(outputFormat);
        doReturn(lock).when(spy).getLockByPrimaryKey(row);

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch ready = new CountDownLatch(numThreads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(
                    () -> {
                        try {
                            ready.countDown();
                            start.await();

                            spy.lockProcess(
                                    row,
                                    () -> {
                                        // do nothing
                                    });

                        } catch (Exception e) {
                            fail(e);
                        } finally {
                            done.countDown();
                        }
                    });
        }

        ready.await();
        start.countDown();
        done.await();
        executor.shutdown();

        verify(lock, times(numThreads)).lock();
        verify(lock, times(numThreads)).unlock();
    }
}

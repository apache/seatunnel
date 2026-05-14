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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DirtyRecordCollectorTest {

    @Test
    void testConcurrentCollect() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        int threadCount = 8;
        int recordsPerThread = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(
                    () -> {
                        try {
                            for (int j = 0; j < recordsPerThread; j++) {
                                collector.collect(0, new SeaTunnelRow(new Object[] {j}), null);
                            }
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
        }

        Assertions.assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        executorService.shutdownNow();
        Assertions.assertEquals(threadCount * recordsPerThread, collector.getDirtyRecordCount());
    }

    @Test
    void testThresholdBoundary() {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        collector.init(ConfigFactory.parseString("threshold=2\nfail_on_threshold=true"));

        Assertions.assertDoesNotThrow(
                () -> collector.collect(0, new SeaTunnelRow(new Object[] {1}), null));
        RuntimeException thresholdException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> collector.collect(0, new SeaTunnelRow(new Object[] {2}), null));
        Assertions.assertTrue(thresholdException.getMessage().contains("threshold exceeded"));

        RuntimeException aboveThresholdException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> collector.collect(0, new SeaTunnelRow(new Object[] {3}), null));
        Assertions.assertTrue(aboveThresholdException.getMessage().contains("threshold exceeded"));
    }

    @Test
    void testNoOpReadResolveSingleton() throws Exception {
        Object restored = roundTrip(NoOpDirtyRecordCollector.INSTANCE);
        Assertions.assertSame(NoOpDirtyRecordCollector.INSTANCE, restored);
    }

    @Test
    void testDecoratorCollectsNonRowPayload() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(new FailingWriter(), collector, 1);

        Assertions.assertDoesNotThrow(() -> writer.write("payload"));
        Assertions.assertEquals(1L, collector.getDirtyRecordCount());
    }

    @Test
    void testValidatingCollectorRequiresValidator() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> new ValidatingDirtyRecordCollector(NoOpDirtyRecordCollector.INSTANCE, null));
    }

    @Test
    void testCollectorServiceDiscovery() {
        DirtyRecordCollector collector =
                DirtyRecordCollectorFactory.createCollector(
                        ConfigFactory.parseString("type=counting"));
        Assertions.assertInstanceOf(CountingDirtyRecordCollector.class, collector);
    }

    @Test
    void testValidatorServiceDiscovery() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", "table"),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        DirtyDataValidator validator =
                DirtyDataValidatorFactory.createValidator(
                        "AlwaysDirtyDataValidator", ConfigFactory.empty(), catalogTable);
        Assertions.assertNotNull(validator);
        Assertions.assertTrue(
                validator.validate(new SeaTunnelRow(new Object[] {1}), catalogTable).isDirty());
    }

    private Object roundTrip(Object value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(value);
        }
        try (ObjectInputStream inputStream =
                new ObjectInputStream(
                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
            return inputStream.readObject();
        }
    }

    private static class FailingWriter implements SinkWriter<String, Void, Void> {

        @Override
        public void write(String element) throws IOException {
            throw new IOException("boom");
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }
}

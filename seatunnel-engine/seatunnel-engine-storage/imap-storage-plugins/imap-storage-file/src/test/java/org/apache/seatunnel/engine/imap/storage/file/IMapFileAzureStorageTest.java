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

package org.apache.seatunnel.engine.imap.storage.file;

import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * IMap persistence with {@code storage.type: azure} against Azurite. A WASB block blob is only
 * committed on close, which is why the {@code hdfs} type loses IMap data on Azure; these tests
 * check that every update is readable by a new storage instance, including while the writing
 * instance is still open (the state a crashed node leaves behind).
 */
@EnabledIf(
        value = "isDockerAvailable",
        disabledReason = "The Azure storage tests run Azurite and need Docker")
@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason =
                "Azurite is a Linux image, and the fixed 127.0.0.1:10000 binding that"
                        + " hadoop-azure's emulator mode requires is often taken on Windows hosts")
public class IMapFileAzureStorageTest {

    private static final String NAMESPACE = "/seatunnel-azurite/imap";
    private static final String CLUSTER = "azurite";
    private static final int ENTRIES = 20;

    // CloudWriter ignores a block.size of 1 MiB or less, so use the smallest accepted value.
    // 24 values of 64 KiB (about 1.5 MiB) then roll the writer over at least once.
    private static final String ROLLOVER_BLOCK_SIZE = String.valueOf(1024 * 1024 + 1);
    private static final int LARGE_ENTRIES = 24;
    private static final int LARGE_VALUE_CHARS = 64 * 1024;

    private static AzuriteEmulator azurite;

    private final List<IMapFileStorage> opened = new ArrayList<>();

    static boolean isDockerAvailable() {
        return AzuriteEmulator.isDockerAvailable();
    }

    @BeforeAll
    static void startAzurite() {
        azurite = AzuriteEmulator.start();
    }

    @AfterAll
    static void stopAzurite() {
        if (azurite != null) {
            azurite.close();
        }
    }

    @Test
    void newInstanceLoadsDataWrittenByAClosedInstance() {
        String business = "closed-" + System.nanoTime();
        IMapFileStorage writer = open(business);
        storeEntries(writer);
        writer.destroy(false);
        // Closed already; the reader's cleanup deletes the shared business directory.
        opened.remove(writer);

        Map<Object, Object> loaded = open(business).loadAll();

        assertAllEntries(loaded);
    }

    @Test
    void newInstanceLoadsDataWhileTheWriterIsStillOpen() {
        String business = "unclosed-" + System.nanoTime();
        IMapFileStorage writer = open(business);
        storeEntries(writer);
        // The writer is deliberately not destroyed, like a node that crashed after these writes.

        Map<Object, Object> loaded = open(business).loadAll();

        assertAllEntries(loaded);
    }

    @Test
    void newInstanceSeesTheLatestUpdateAndDelete() {
        String business = "mutations-" + System.nanoTime();
        IMapFileStorage writer = open(business);
        storeEntries(writer);
        Assertions.assertTrue(writer.store("k0", -1L));
        Assertions.assertTrue(writer.delete("k1"));

        Map<Object, Object> loaded = open(business).loadAll();

        Assertions.assertEquals(ENTRIES - 1, loaded.size(), "loaded keys: " + loaded.keySet());
        Assertions.assertEquals(-1L, loaded.get("k0"));
        Assertions.assertFalse(loaded.containsKey("k1"));
    }

    @Test
    void newInstanceLoadsEverySegmentAfterRolloverWhileTheWriterIsStillOpen() throws IOException {
        String business = "rollover-" + System.nanoTime();
        // block.size is held by the shared AZURE configuration, so set it explicitly here
        // instead of relying on whatever another test in this JVM left behind.
        IMapFileStorage writer = open(business, ROLLOVER_BLOCK_SIZE);
        String payload = payload();
        for (int i = 0; i < LARGE_ENTRIES; i++) {
            Assertions.assertTrue(writer.store("k" + i, i + payload), "store failed for k" + i);
        }
        // k0 lives in the first segment; this update is written to a later one.
        Assertions.assertTrue(writer.store("k0", "updated"));
        // The writer is deliberately not destroyed, like a node that crashed after these writes.

        FileStatus[] segments =
                writer.fs.listStatus(
                        new Path(NAMESPACE + "/" + CLUSTER + "/" + business + "/" + writer.region));
        Assertions.assertTrue(
                segments.length >= 2,
                "expected the writer to roll over into a new object, found: "
                        + Arrays.stream(segments)
                                .map(segment -> segment.getPath().getName())
                                .collect(Collectors.toList()));

        Map<Object, Object> loaded = open(business).loadAll();

        Assertions.assertEquals(LARGE_ENTRIES, loaded.size(), "loaded keys: " + loaded.keySet());
        Assertions.assertEquals("updated", loaded.get("k0"));
        for (int i = 1; i < LARGE_ENTRIES; i++) {
            Assertions.assertEquals(i + payload, loaded.get("k" + i), "wrong value for k" + i);
        }
    }

    @AfterEach
    void cleanUp() {
        for (IMapFileStorage storage : opened) {
            storage.destroy(true);
        }
        opened.clear();
    }

    private IMapFileStorage open(String business) {
        return open(business, null);
    }

    private IMapFileStorage open(String business, String blockSize) {
        Map<String, Object> properties = new HashMap<>();
        if (blockSize != null) {
            properties.put("block.size", blockSize);
        }
        properties.put("storage.type", "azure");
        properties.put("azure.container", AzuriteEmulator.CONTAINER_URI);
        properties.put(FileConstants.FileInitProperties.NAMESPACE_KEY, NAMESPACE);
        properties.put(FileConstants.FileInitProperties.CLUSTER_NAME, CLUSTER);
        properties.put(FileConstants.FileInitProperties.BUSINESS_KEY, business);
        properties.put(
                FileConstants.FileInitProperties.WRITE_DATA_TIMEOUT_MILLISECONDS_KEY, 10000L);
        IMapFileStorage storage = new IMapFileStorage();
        storage.initialize(properties);
        opened.add(storage);
        return storage;
    }

    private static void storeEntries(IMapFileStorage storage) {
        for (long i = 0; i < ENTRIES; i++) {
            Assertions.assertTrue(storage.store("k" + i, i), "store failed for k" + i);
        }
    }

    private static String payload() {
        char[] chars = new char[LARGE_VALUE_CHARS];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    private static void assertAllEntries(Map<Object, Object> loaded) {
        Assertions.assertEquals(ENTRIES, loaded.size(), "loaded keys: " + loaded.keySet());
        for (long i = 0; i < ENTRIES; i++) {
            Assertions.assertEquals(i, loaded.get("k" + i));
        }
    }
}

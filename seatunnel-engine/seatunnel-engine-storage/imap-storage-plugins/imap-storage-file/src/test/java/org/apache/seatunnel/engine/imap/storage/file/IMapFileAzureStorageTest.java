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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * IMap persistence with {@code storage.type: azure} against Azurite. A WASB block blob is only
 * committed on close, which is why the {@code hdfs} type loses IMap data on Azure; these tests
 * check that every update is readable by a new storage instance, including while the writing
 * instance is still open (the state a crashed node leaves behind).
 */
@EnabledIf(
        value = "isDockerAvailable",
        disabledReason = "The Azure storage tests run Azurite and need Docker")
public class IMapFileAzureStorageTest {

    private static final String NAMESPACE = "/seatunnel-azurite/imap";
    private static final String CLUSTER = "azurite";
    private static final int ENTRIES = 20;

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

    @AfterEach
    void cleanUp() {
        for (IMapFileStorage storage : opened) {
            storage.destroy(true);
        }
        opened.clear();
    }

    private IMapFileStorage open(String business) {
        Map<String, Object> properties = new HashMap<>();
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

    private static void assertAllEntries(Map<Object, Object> loaded) {
        Assertions.assertEquals(ENTRIES, loaded.size(), "loaded keys: " + loaded.keySet());
        for (long i = 0; i < ENTRIES; i++) {
            Assertions.assertEquals(i, loaded.get("k" + i));
        }
    }
}

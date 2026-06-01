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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.core.HazelcastInstance;

import java.nio.file.Path;
import java.util.Properties;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for FileMapStore.
 *
 * <p>Covers two scenarios introduced/improved by the fix for
 * https://github.com/apache/seatunnel/issues/10883:
 *
 * <ol>
 *   <li>init() with a valid local-fs config succeeds end-to-end (SPI discovery + storage init).
 *   <li>init() with an unknown storage type throws immediately instead of silently failing.
 * </ol>
 *
 * <p>Note: HDFS/S3/OSS backends are NOT tested here because they require Hadoop uber jars and
 * remote infrastructure. Those are covered by IMapFileStorageTest. What we verify here is that the
 * SPI discovery path (FactoryUtil.discoverFactory) is wired correctly through FileMapStore, and
 * that failures surface as exceptions rather than silent no-ops.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileMapStoreTest {

    @TempDir Path tempDir;

    /**
     * Verifies that FileMapStore.init() can successfully resolve the "hdfs" factory via SPI and
     * initialize local-filesystem storage without errors.
     *
     * <p>This test fails if ServicesResourceTransformer is absent from the shade config, because
     * ServiceLoader would return no IMapStorageFactory implementations.
     */
    @Test
    public void testInitSucceedsWithLocalFileSystem() {
        FileMapStore store = new FileMapStore();
        Properties props = buildLocalFsProperties(tempDir.toString());

        Assertions.assertDoesNotThrow(
                () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                "FileMapStore.init() should succeed with local-fs config. "
                        + "If this fails with 'Could not find any factories', "
                        + "ServicesResourceTransformer is likely missing from seatunnel-starter shade config.");

        store.destroy();
    }

    /**
     * Verifies that FileMapStore.init() throws (rather than silently swallowing) when the storage
     * type is unknown.
     *
     * <p>Before the fix, Hazelcast's MapLoader lifecycle could absorb the exception and leave
     * mapStorage null, causing NullPointerException on the first store/load call with no indication
     * of the real root cause.
     */
    @Test
    public void testInitThrowsOnUnknownStorageType() {
        FileMapStore store = new FileMapStore();
        Properties props = new Properties();
        props.setProperty("type", "non-existent-storage-type");

        RuntimeException ex =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> store.init(mock(HazelcastInstance.class), props, "test-map"),
                        "FileMapStore.init() must throw on unknown storage type instead of silently failing.");

        String msg = ex.getMessage() != null ? ex.getMessage() : "";
        Assertions.assertTrue(
                msg.contains("non-existent-storage-type")
                        || msg.contains("Could not find any factories"),
                "Exception message should identify the unknown type. Got: " + msg);
    }

    private Properties buildLocalFsProperties(String namespace) {
        Properties props = new Properties();
        // "hdfs" is the factoryIdentifier of IMapFileStorageFactory — covers local fs via
        // fs.defaultFS=file:///
        props.setProperty("type", "hdfs");
        props.setProperty("fs.defaultFS", "file:///");
        props.setProperty("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        props.setProperty(FileConstants.FileInitProperties.BUSINESS_KEY, "test");
        props.setProperty(FileConstants.FileInitProperties.NAMESPACE_KEY, namespace);
        props.setProperty(FileConstants.FileInitProperties.CLUSTER_NAME, "test-cluster");
        return props;
    }
}

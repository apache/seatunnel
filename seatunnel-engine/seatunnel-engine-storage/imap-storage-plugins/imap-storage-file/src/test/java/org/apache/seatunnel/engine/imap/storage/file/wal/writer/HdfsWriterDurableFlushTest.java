/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.wal.writer;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALReader;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

/**
 * Verifies mid-stream write-then-read visibility across handles while the writer is still open.
 *
 * <p>This is not a crash-survival / fsync proof: same-process read-back also passes for {@code
 * hflush()}-only data sitting in the OS page cache. Use {@link HdfsWriterFlushSyncPathTest} to
 * assert that {@link HdfsWriter#flush()} still invokes an {@code hsync}-family method exactly once.
 */
@EnabledOnOs({LINUX, MAC})
class HdfsWriterDurableFlushTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void writeShouldPersistRecordsWithSingleSyncPath() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.getLocal(conf);
        Serializer serializer = new ProtoStuffSerializer();
        Path parentPath = new Path(tempDir.resolve("wal").toUri());
        WALReader reader = new WALReader(fs, FileConfiguration.HDFS, serializer);

        try (HdfsWriter writer = new HdfsWriter()) {
            writer.initialize(fs, parentPath, serializer);
            for (int index = 0; index < 8; index++) {
                writer.write(
                        IMapFileData.builder()
                                .key(serializer.serialize("key-" + index))
                                .keyClassName(String.class.getName())
                                .value(serializer.serialize(index))
                                .valueClassName(Integer.class.getName())
                                .timestamp(System.nanoTime())
                                .deleted(false)
                                .build());

                // Assert durability after each append, before close(), so the test covers the
                // write-through wait path rather than only close-time visibility.
                if (index == 3 || index == 7) {
                    Map<Object, Object> loaded = reader.loadAllData(parentPath, new HashSet<>());
                    Assertions.assertEquals(index + 1, loaded.size());
                    for (int verified = 0; verified <= index; verified++) {
                        Assertions.assertEquals(verified, loaded.get("key-" + verified));
                    }
                }
            }
        }

        Assertions.assertTrue(Files.exists(tempDir.resolve("wal").resolve(IFileWriter.FILE_NAME)));

        Map<Object, Object> loadedAfterClose = reader.loadAllData(parentPath, new HashSet<>());
        Assertions.assertEquals(8, loadedAfterClose.size());
        for (int index = 0; index < 8; index++) {
            Assertions.assertEquals(index, loadedAfterClose.get("key-" + index));
        }
    }
}

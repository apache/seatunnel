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

package org.apache.seatunnel.engine.imap.storage.file.wal.reader;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.WALDataUtils;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

/**
 * Documents that {@link DefaultReader} stops at an incomplete <em>trailing</em> WAL frame so prior
 * complete records remain recoverable. This does not cover a torn frame followed by later bytes —
 * see {@link DefaultReaderTornMidFileRecordTest} and the fail-closed APPEND policy in {@code
 * WALWorkHandler}.
 */
@EnabledOnOs({LINUX, MAC})
class DefaultReaderTornTrailingRecordTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void readAllDataShouldSkipIncompleteTrailingFrame() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.setWriteChecksum(false);
        Serializer serializer = new ProtoStuffSerializer();
        Path parentPath = new Path(tempDir.resolve("wal").toUri());
        Path walPath = new Path(parentPath, IFileWriter.FILE_NAME);

        IMapFileData first =
                IMapFileData.builder()
                        .key(serializer.serialize("key-0"))
                        .keyClassName(String.class.getName())
                        .value(serializer.serialize(0))
                        .valueClassName(Integer.class.getName())
                        .timestamp(System.nanoTime())
                        .deleted(false)
                        .build();
        IMapFileData second =
                IMapFileData.builder()
                        .key(serializer.serialize("key-1"))
                        .keyClassName(String.class.getName())
                        .value(serializer.serialize(1))
                        .valueClassName(Integer.class.getName())
                        .timestamp(System.nanoTime())
                        .deleted(false)
                        .build();

        byte[] firstFrame = WALDataUtils.wrapperBytes(serializer.serialize(first));
        byte[] secondFrame = WALDataUtils.wrapperBytes(serializer.serialize(second));
        // Keep length prefix + half the payload so the reader sees a torn trailer.
        int tornLength = WALDataUtils.WAL_DATA_METADATA_LENGTH + secondFrame.length / 4;
        Assertions.assertTrue(tornLength < secondFrame.length);

        try (FSDataOutputStream out = fs.create(walPath)) {
            out.write(firstFrame);
            out.write(secondFrame, 0, tornLength);
            out.hsync();
        }

        DefaultReader reader = new DefaultReader();
        reader.initialize(fs, serializer);
        List<IMapFileData> loaded = reader.readAllData(parentPath);

        Assertions.assertEquals(1, loaded.size());
        Assertions.assertArrayEquals(first.getKey(), loaded.get(0).getKey());
        Assertions.assertArrayEquals(first.getValue(), loaded.get(0).getValue());
    }
}

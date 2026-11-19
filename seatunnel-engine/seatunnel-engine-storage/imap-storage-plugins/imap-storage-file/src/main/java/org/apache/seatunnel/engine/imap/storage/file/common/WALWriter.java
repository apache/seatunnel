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

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionCoordinator;
import org.apache.seatunnel.engine.imap.storage.file.compaction.HdfsCompactionStorage;
import org.apache.seatunnel.engine.imap.storage.file.compaction.RegionCompactionPlanner;
import org.apache.seatunnel.engine.imap.storage.file.config.CompactionConfig;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.SegmentedHdfsWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

public class WALWriter implements AutoCloseable {

    IFileWriter writer;
    private Closeable compactionRegistration;

    public WALWriter(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            Path parentPath,
            Serializer serializer)
            throws IOException {
        this(fs, fileConfiguration, parentPath, serializer, CompactionConfig.disabled());
    }

    public WALWriter(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            Path parentPath,
            Serializer serializer,
            CompactionConfig compaction)
            throws IOException {
        RegionCompactionPlanner planner = null;
        HdfsCompactionStorage storage = null;
        if (compaction.isEnabled()) {
            if (fileConfiguration != FileConfiguration.HDFS) {
                throw new IllegalArgumentException(
                        "compaction.enabled currently supports storage.type=hdfs only");
            }
            storage = new HdfsCompactionStorage(fs, serializer);
            storage.initialize(parentPath);
            planner = new RegionCompactionPlanner(parentPath);
            this.writer = new SegmentedHdfsWriter(compaction.getSegmentSize(), planner);
        } else {
            this.writer = DiscoveryWalFileFactory.getWriter(fileConfiguration.getName());
        }
        this.writer.setBlockSize(fileConfiguration.getConfiguration().getBlockSize());
        this.writer.initialize(fs, parentPath, serializer);
        if (planner != null) {
            compactionRegistration =
                    CompactionCoordinator.register(
                            planner, storage, compaction.getIntervalSeconds());
        }
    }

    public void write(IMapFileData data) throws IOException {
        this.writer.write(data);
    }

    @Override
    public void close() throws Exception {
        stopCompaction();
        this.writer.close();
    }

    /** Waits for compaction outside the Disruptor shutdown deadline before closing the writer. */
    public void stopCompaction() throws IOException {
        if (compactionRegistration != null) {
            compactionRegistration.close();
        }
    }
}

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
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.imap.storage.file.wal.DiscoveryWalFileFactory;
import org.apache.seatunnel.engine.imap.storage.file.wal.writer.IFileWriter;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WALWriter implements AutoCloseable {

    IFileWriter writer;

    public WALWriter(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            Path parentPath,
            Serializer serializer)
            throws IOException {
        this.writer = DiscoveryWalFileFactory.getWriter(fileConfiguration.getName());
        this.writer.setBlockSize(fileConfiguration.getConfiguration().getBlockSize());
        this.writer.initialize(fs, parentPath, serializer);
    }

    public void write(IMapFileData data) throws IOException {
        this.writer.write(data);
    }

    @Override
    public void close() throws Exception {
        this.writer.close();
    }
}

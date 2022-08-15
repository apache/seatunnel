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

package org.apache.seatunnel.connectors.seatunnel.file.local.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.filesystem.LocalFileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.filesystem.LocalFileSystemCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.writer.LocalTransactionStateFileWriteFactory;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.SinkFileSystemPlugin;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;

import java.util.List;
import java.util.Optional;

public class LocalFileSinkPlugin implements SinkFileSystemPlugin {
    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public Optional<TransactionStateFileWriter> getTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                                                              @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                                                              @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                                                              @NonNull List<Integer> sinkColumnsIndexInRow,
                                                                              @NonNull String tmpPath,
                                                                              @NonNull String targetPath,
                                                                              @NonNull String jobId,
                                                                              int subTaskIndex,
                                                                              @NonNull String fieldDelimiter,
                                                                              @NonNull String rowDelimiter,
                                                                              @NonNull FileSystem fileSystem) {
        // using factory to generate transaction state file writer
        TransactionStateFileWriter writer = LocalTransactionStateFileWriteFactory.of(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fieldDelimiter, rowDelimiter, fileSystem);
        return Optional.of(writer);
    }

    @Override
    public Optional<FileSystemCommitter> getFileSystemCommitter() {
        return Optional.of(new LocalFileSystemCommitter());
    }

    @Override
    public Optional<FileSystem> getFileSystem() {
        return Optional.of(new LocalFileSystem());
    }
}

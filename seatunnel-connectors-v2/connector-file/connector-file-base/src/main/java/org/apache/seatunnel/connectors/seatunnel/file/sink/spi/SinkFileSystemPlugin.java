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

package org.apache.seatunnel.connectors.seatunnel.file.sink.spi;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface SinkFileSystemPlugin extends Serializable {

    String getPluginName();

    /**
     * Implements this method and return a class which is implement the interface {@link TransactionStateFileWriter}
     */
    Optional<TransactionStateFileWriter> getTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                                                       @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                                                       @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                                                       @NonNull List<Integer> sinkColumnsIndexInRow,
                                                                       @NonNull String tmpPath,
                                                                       @NonNull String targetPath,
                                                                       @NonNull String jobId,
                                                                       int subTaskIndex,
                                                                       @NonNull String fieldDelimiter,
                                                                       @NonNull String rowDelimiter,
                                                                       @NonNull FileSystem fileSystem);

    Optional<FileSystemCommitter> getFileSystemCommitter();

    Optional<FileSystem> getFileSystem();
}

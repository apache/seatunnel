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

package org.apache.seatunnel.connectors.seatunnel.file.sink.hdfs;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkTransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;

import java.util.List;

public class HdfsTransactionStateFileWriteFactory {

    private HdfsTransactionStateFileWriteFactory() {}

    public static TransactionStateFileWriter of(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
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
        FileSinkTransactionFileNameGenerator fileSinkTransactionFileNameGenerator = (FileSinkTransactionFileNameGenerator) transactionFileNameGenerator;
        FileFormat fileFormat = fileSinkTransactionFileNameGenerator.getFileFormat();
        if (fileFormat.equals(FileFormat.CSV)) {
            // #2133 wait this issue closed, there will be replaced using csv writer
            return new HdfsTxtTransactionStateFileWriter(
                    seaTunnelRowTypeInfo,
                    transactionFileNameGenerator,
                    partitionDirNameGenerator,
                    sinkColumnsIndexInRow,
                    tmpPath,
                    targetPath,
                    jobId,
                    subTaskIndex,
                    fieldDelimiter,
                    rowDelimiter,
                    fileSystem);
        }
        if (fileFormat.equals(FileFormat.PARQUET)) {
            return new HdfsParquetTransactionStateFileWriter(
                    seaTunnelRowTypeInfo,
                    transactionFileNameGenerator,
                    partitionDirNameGenerator,
                    sinkColumnsIndexInRow,
                    tmpPath,
                    targetPath,
                    jobId,
                    subTaskIndex,
                    fileSystem);
        }
        if (fileFormat.equals(FileFormat.ORC)) {
            return new HdfsOrcTransactionStateFileWriter(
                    seaTunnelRowTypeInfo,
                    transactionFileNameGenerator,
                    partitionDirNameGenerator,
                    sinkColumnsIndexInRow,
                    tmpPath,
                    targetPath,
                    jobId,
                    subTaskIndex,
                    fileSystem);
        }
        // if file type not supported by file connector, default txt writer will be generated
        return new HdfsTxtTransactionStateFileWriter(
                    seaTunnelRowTypeInfo,
                    transactionFileNameGenerator,
                    partitionDirNameGenerator,
                    sinkColumnsIndexInRow,
                    tmpPath,
                    targetPath,
                    jobId,
                    subTaskIndex,
                    fieldDelimiter,
                    rowDelimiter,
                    fileSystem);
    }
}

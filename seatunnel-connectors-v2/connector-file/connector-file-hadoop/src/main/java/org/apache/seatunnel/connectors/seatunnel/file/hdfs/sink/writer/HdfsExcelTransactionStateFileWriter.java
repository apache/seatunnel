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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractTransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsExcelTransactionStateFileWriter extends AbstractTransactionStateFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsExcelTransactionStateFileWriter.class);
    private Map<String, ExcelGenerator> beingWrittenWriter;

    public HdfsExcelTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                               @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                               @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                               @NonNull List<Integer> sinkColumnsIndexInRow,
                                               @NonNull String tmpPath,
                                               @NonNull String targetPath,
                                               @NonNull String jobId,
                                               int subTaskIndex,
                                               @NonNull FileSystem fileSystem) {
        super(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fileSystem);
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ExcelGenerator excelGenerator = getOrCreateExcelGenerator(filePath);
        excelGenerator.writeData(seaTunnelRow);
    }

    @Override
    public void finishAndCloseWriteFile() {
        this.beingWrittenWriter.forEach((k, v) -> {
            try {
                FileSystemUtils.createFile(k);
                FSDataOutputStream outputStream = FileSystemUtils.getOutputStream(k);
                v.flushAndCloseExcel(outputStream);
                outputStream.close();
            } catch (IOException e) {
                LOGGER.error("can not get output file stream");
                throw new RuntimeException(e);
            }
            needMoveFiles.put(k, getTargetLocation(k));
        });
    }

    @Override
    public void beginTransaction(String transactionId) {
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void abortTransaction(String transactionId) {
        this.beingWrittenWriter = new HashMap<>();
    }

    private ExcelGenerator getOrCreateExcelGenerator(@NonNull String filePath) {
        ExcelGenerator excelGenerator = this.beingWrittenWriter.get(filePath);
        if (excelGenerator == null) {
            excelGenerator = new ExcelGenerator(sinkColumnsIndexInRow, seaTunnelRowTypeInfo);
            this.beingWrittenWriter.put(filePath, excelGenerator);
        }
        return excelGenerator;
    }

}

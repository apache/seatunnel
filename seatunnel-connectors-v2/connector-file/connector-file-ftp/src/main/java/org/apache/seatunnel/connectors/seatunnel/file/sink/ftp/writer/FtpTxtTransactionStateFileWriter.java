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

package org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.ftp.util.FtpFileUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractTransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FtpTxtTransactionStateFileWriter extends AbstractTransactionStateFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FtpTxtTransactionStateFileWriter.class);

    private Map<String, OutputStream> beingWrittenOutputStream;
    private String fieldDelimiter;
    private String rowDelimiter;

    public FtpTxtTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                            @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                            @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                            @NonNull List<Integer> sinkColumnsIndexInRow,
                                            @NonNull String tmpPath,
                                            @NonNull String targetPath,
                                            @NonNull String jobId,
                                            int subTaskIndex,
                                            @NonNull String fieldDelimiter,
                                            @NonNull String rowDelimiter,
                                            @NonNull FileSystem fileSystem
    ) {
        super(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fileSystem);
        this.fieldDelimiter = fieldDelimiter;
        this.rowDelimiter = rowDelimiter;
    }

    @Override
    public void beginTransaction(String transactionId) {
        this.beingWrittenOutputStream = new HashMap<>();
    }

    @Override
    public void abortTransaction(String transactionId) {
        this.beingWrittenOutputStream = new HashMap<>();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        OutputStream outputStream = getOrCreateOutputStream(filePath);
        String line = transformRowToLine(seaTunnelRow);
        try {
            outputStream.write(line.getBytes());
            outputStream.write(rowDelimiter.getBytes());
        } catch (IOException e) {
            LOGGER.error("write data to file {} error", filePath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishAndCloseWriteFile() {
        beingWrittenOutputStream.entrySet().forEach(entry -> {
            try {
                entry.getValue().flush();
            } catch (IOException e) {
                LOGGER.error("error when flush file {}", entry.getKey());
                throw new RuntimeException(e);
            } finally {
                try {
                    entry.getValue().close();
                } catch (IOException e) {
                    LOGGER.error("error when close output stream {}", entry.getKey());
                }
            }
            needMoveFiles.put(entry.getKey(), getTargetLocation(entry.getKey()));
        });
    }

    private OutputStream getOrCreateOutputStream(@NonNull String filePath) {
        OutputStream outputStream = beingWrittenOutputStream.get(filePath);
        if (outputStream == null) {
            try {
                outputStream =  FtpFileUtils.getOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, outputStream);
            } catch (IOException e) {
                LOGGER.error("can not get output file stream");
                throw new RuntimeException(e);
            }
        }
        return outputStream;
    }

    private String transformRowToLine(@NonNull SeaTunnelRow seaTunnelRow) {
        return this.sinkColumnsIndexInRow.stream().map(index -> seaTunnelRow.getFields()[index] == null ? "" : seaTunnelRow.getFields()[index].toString()).collect(Collectors.joining(fieldDelimiter));
    }
}

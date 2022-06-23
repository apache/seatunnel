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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.utils.HdfsUtils;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HdfsTxtTransactionStateFileWriter extends AbstractTransactionStateFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTxtTransactionStateFileWriter.class);
    private Map<String, FSDataOutputStream> beingWrittenOutputStream;

    private String fieldDelimiter;
    private String rowDelimiter;

    public HdfsTxtTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                             @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                             @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                             @NonNull List<Integer> sinkColumnsIndexInRow,
                                             @NonNull String tmpPath,
                                             @NonNull String targetPath,
                                             @NonNull String jobId,
                                             int subTaskIndex,
                                             @NonNull String fieldDelimiter,
                                             @NonNull String rowDelimiter) {
        super(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex);

        this.fieldDelimiter = fieldDelimiter;
        this.rowDelimiter = rowDelimiter;
        beingWrittenOutputStream = new HashMap<>();
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
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        String line = transformRowToLine(seaTunnelRow);
        try {
            fsDataOutputStream.write(line.getBytes());
            fsDataOutputStream.write(rowDelimiter.getBytes());
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

    private FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                fsDataOutputStream = HdfsUtils.getOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
            } catch (IOException e) {
                LOGGER.error("can not get output file stream");
                throw new RuntimeException(e);
            }
        }
        return fsDataOutputStream;
    }

    private String transformRowToLine(@NonNull SeaTunnelRow seaTunnelRow) {
        return this.sinkColumnsIndexInRow.stream()
            .map(index -> seaTunnelRow.getFields()[index] == null ? "" : seaTunnelRow.getFields()[index].toString())
            .collect(Collectors.joining(fieldDelimiter));
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.file.sink.local;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractTransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class LocalJsonTransactionStateFileWriter extends AbstractTransactionStateFileWriter {

    private static final long serialVersionUID = -3834472539886339383L;

    private final byte[] rowDelimiter;
    private final SerializationSchema serializationSchema;
    private Map<String, FileOutputStream> beingWrittenOutputStream;

    public LocalJsonTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                               @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                               @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                               @NonNull List<Integer> sinkColumnsIndexInRow,
                                               @NonNull String tmpPath,
                                               @NonNull String targetPath,
                                               @NonNull String jobId,
                                               int subTaskIndex,
                                               @NonNull String rowDelimiter,
                                               @NonNull FileSystem fileSystem) {
        super(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fileSystem);

        this.rowDelimiter = rowDelimiter.getBytes();
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowTypeInfo);
        this.beingWrittenOutputStream = new HashMap<>();
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
        FileOutputStream fileOutputStream = getOrCreateOutputStream(filePath);
        try {
            byte[] rowBytes = serializationSchema.serialize(seaTunnelRow);
            fileOutputStream.write(rowBytes);
            fileOutputStream.write(rowDelimiter);
        } catch (IOException e) {
            log.error("write data to file {} error", filePath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishAndCloseWriteFile() {
        beingWrittenOutputStream.entrySet().forEach(entry -> {
            try {
                entry.getValue().flush();
            } catch (IOException e) {
                log.error("error when flush file {}", entry.getKey());
                throw new RuntimeException(e);
            } finally {
                try {
                    entry.getValue().close();
                } catch (IOException e) {
                    log.error("error when close output stream {}", entry.getKey());
                }
            }

            needMoveFiles.put(entry.getKey(), getTargetLocation(entry.getKey()));
        });
    }

    private FileOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FileOutputStream fileOutputStream = beingWrittenOutputStream.get(filePath);
        if (fileOutputStream == null) {
            try {
                FileUtils.createFile(filePath);
                fileOutputStream = new FileOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, fileOutputStream);
            } catch (IOException e) {
                log.error("can not get output file stream");
                throw new RuntimeException(e);
            }
        }
        return fileOutputStream;
    }
}

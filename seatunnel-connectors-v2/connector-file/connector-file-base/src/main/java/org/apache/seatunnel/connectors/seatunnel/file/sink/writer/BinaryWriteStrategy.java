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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.BinaryReadStrategy;

import org.apache.hadoop.fs.FSDataOutputStream;

import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;

public class BinaryWriteStrategy extends AbstractWriteStrategy<FSDataOutputStream> {

    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final LinkedHashMap<String, Long> partIndexMap;

    public BinaryWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.partIndexMap = new LinkedHashMap<>();
        if (fileSinkConfig.isCreateEmptyFileWhenNoData()) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                    "BinaryWriteStrategy does not support generating empty files when no data is written.");
        }
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        super.setCatalogTable(catalogTable);
        if (!catalogTable.getSeaTunnelRowType().equals(BinaryReadStrategy.binaryRowType)) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                    "BinaryWriteStrategy only supports binary format, please read file with `BINARY` format, and do not change schema in the transform.");
        }
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        byte[] data = (byte[]) seaTunnelRow.getField(0);
        String relativePath = (String) seaTunnelRow.getField(1);
        long partIndex = (long) seaTunnelRow.getField(2);
        String filePath = getOrCreateFilePathBeingWritten(relativePath);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        if (partIndex - 1 != partIndexMap.get(filePath)) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.BINARY_FILE_PART_ORDER_ERROR,
                    "Last order is " + partIndexMap.get(filePath) + ", but get " + partIndex);
        } else {
            partIndexMap.put(filePath, partIndex);
        }
        try {
            fsDataOutputStream.write(data);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("BinaryFile", "write", filePath, e);
        }
    }

    public String getOrCreateFilePathBeingWritten(String relativePath) {
        String beingWrittenFilePath = beingWrittenFile.get(relativePath);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments = new String[] {transactionDirectory, relativePath};
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(relativePath, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }

    @Override
    public FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                partIndexMap.put(filePath, -1L);
            } catch (IOException e) {
                throw CommonError.fileOperationFailed("BinaryFile", "open", filePath, e);
            }
        }
        return fsDataOutputStream;
    }

    @Override
    public void finishAndCloseFile() {
        beingWrittenOutputStream.forEach(
                (key, value) -> {
                    try {
                        value.flush();
                    } catch (IOException e) {
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    } finally {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("error when close output stream {}", key, e);
                        }
                    }
                    needMoveFiles.put(key, getTargetLocation(key));
                });
        beingWrittenOutputStream.clear();
        partIndexMap.clear();
    }
}

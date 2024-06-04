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

import org.apache.seatunnel.api.table.type.BinaryObject;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.hadoop.fs.FSDataOutputStream;

import io.airlift.compress.lzo.LzopCodec;
import lombok.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class BinaryWriteStrategy extends AbstractWriteStrategy {
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final Map<String, Boolean> isFirstWrite;

    public BinaryWriteStrategy(FileSinkConfig textFileSinkConfig) {
        super(textFileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.isFirstWrite = new HashMap<>();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = null;
        FSDataOutputStream fsDataOutputStream;
        try {
            BinaryObject binaryObject = (BinaryObject) seaTunnelRow.getField(0);
            String filename = binaryObject.getName();
            byte[] bytes = Base64.getDecoder().decode(binaryObject.getBytes());
            int len = binaryObject.getLen();

            filePath = getOrCreateFilePathBeingWritten(filename);
            fsDataOutputStream = getOrCreateOutputStream(filePath);

            if (isFirstWrite.get(filePath)) {
                isFirstWrite.put(filePath, false);
            }

            if (len != -1) {
                fsDataOutputStream.write(bytes, 0, len);
            }
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("BinaryFile", "write", filePath, e);
        }
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
                            log.warn("Close file output stream {} failed", key, e);
                        }
                    }
                });
        beingWrittenOutputStream.clear();
        isFirstWrite.clear();
    }

    private FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                switch (compressFormat) {
                    case LZO:
                        LzopCodec lzo = new LzopCodec();
                        OutputStream out =
                                lzo.createOutputStream(
                                        hadoopFileSystemProxy.getOutputStream(
                                                getTargetLocation(filePath)));
                        fsDataOutputStream = new FSDataOutputStream(out, null);
                        break;
                    case NONE:
                        fsDataOutputStream =
                                hadoopFileSystemProxy.getOutputStream(getTargetLocation(filePath));
                        break;
                    default:
                        log.warn(
                                "Binary file does not support this compress type: {}",
                                compressFormat.getCompressCodec());
                        fsDataOutputStream =
                                hadoopFileSystemProxy.getOutputStream(getTargetLocation(filePath));
                        break;
                }
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (IOException e) {
                throw CommonError.fileOperationFailed("BinaryFile", "open", filePath, e);
            }
        }
        return fsDataOutputStream;
    }
}

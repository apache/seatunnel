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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.hadoop.fs.FSDataOutputStream;

import io.airlift.compress.lzo.LzopCodec;
import lombok.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TextWriteStrategy extends AbstractWriteStrategy {
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final Map<String, Boolean> isFirstWrite;
    private final String fieldDelimiter;
    private final String rowDelimiter;
    private final DateUtils.Formatter dateFormat;
    private final DateTimeUtils.Formatter dateTimeFormat;
    private final TimeUtils.Formatter timeFormat;
    private final FileFormat fileFormat;
    private final Boolean enableHeaderWriter;
    private SerializationSchema serializationSchema;

    public TextWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.isFirstWrite = new HashMap<>();
        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();
        this.rowDelimiter = fileSinkConfig.getRowDelimiter();
        this.dateFormat = fileSinkConfig.getDateFormat();
        this.dateTimeFormat = fileSinkConfig.getDatetimeFormat();
        this.timeFormat = fileSinkConfig.getTimeFormat();
        this.fileFormat = fileSinkConfig.getFileFormat();
        this.enableHeaderWriter = fileSinkConfig.getEnableHeaderWriter();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        this.serializationSchema =
                TextSerializationSchema.builder()
                        .seaTunnelRowType(
                                buildSchemaWithRowType(seaTunnelRowType, sinkColumnsIndexInRow))
                        .delimiter(fieldDelimiter)
                        .dateFormatter(dateFormat)
                        .dateTimeFormatter(dateTimeFormat)
                        .timeFormatter(timeFormat)
                        .build();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        try {
            if (isFirstWrite.get(filePath)) {
                isFirstWrite.put(filePath, false);
            } else {
                fsDataOutputStream.write(rowDelimiter.getBytes());
            }
            fsDataOutputStream.write(
                    serializationSchema.serialize(
                            seaTunnelRow.copy(
                                    sinkColumnsIndexInRow.stream()
                                            .mapToInt(Integer::intValue)
                                            .toArray())));
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    String.format("Write data to file [%s] failed", filePath),
                    e);
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
                                CommonErrorCode.FLUSH_DATA_FAILED,
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
                                lzo.createOutputStream(fileSystemUtils.getOutputStream(filePath));
                        fsDataOutputStream = new FSDataOutputStream(out, null);
                        enableWriteHeader(fsDataOutputStream);
                        break;
                    case NONE:
                        fsDataOutputStream = fileSystemUtils.getOutputStream(filePath);
                        enableWriteHeader(fsDataOutputStream);
                        break;
                    default:
                        log.warn(
                                "Text file does not support this compress type: {}",
                                compressFormat.getCompressCodec());
                        fsDataOutputStream = fileSystemUtils.getOutputStream(filePath);
                        enableWriteHeader(fsDataOutputStream);
                        break;
                }
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (IOException e) {
                throw new FileConnectorException(
                        CommonErrorCode.FILE_OPERATION_FAILED,
                        String.format("Open file output stream [%s] failed", filePath),
                        e);
            }
        }
        return fsDataOutputStream;
    }

    private void enableWriteHeader(FSDataOutputStream fsDataOutputStream) throws IOException {
        if (enableHeaderWriter) {
            fsDataOutputStream.write(
                    String.join(
                                    FileFormat.CSV.equals(fileFormat) ? "," : fieldDelimiter,
                                    seaTunnelRowType.getFieldNames())
                            .getBytes());
            fsDataOutputStream.write(rowDelimiter.getBytes());
        }
    }
}

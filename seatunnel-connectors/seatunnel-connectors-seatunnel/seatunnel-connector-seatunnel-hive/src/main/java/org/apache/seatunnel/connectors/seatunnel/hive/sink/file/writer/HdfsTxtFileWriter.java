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

package org.apache.seatunnel.connectors.seatunnel.hive.sink.file.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkConfig;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HdfsTxtFileWriter extends AbstractFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTxtFileWriter.class);

    private Map<String, FSDataOutputStream> beingWrittenOutputStream;

    public HdfsTxtFileWriter(SeaTunnelRowTypeInfo seaTunnelRowTypeInfo,
                             HiveSinkConfig hiveSinkConfig,
                             long sinkId,
                             int subTaskIndex) {
        super(seaTunnelRowTypeInfo, hiveSinkConfig, sinkId, subTaskIndex);
        beingWrittenOutputStream = new HashMap<>();
    }

    @Override
    public String getFileSuffix() {
        return "txt";
    }

    @Override
    public void resetMoreFileWriter(String checkpointId) {
        this.beingWrittenOutputStream = new HashMap<>();
    }

    @Override
    public void abortMore() {
        // delete files
        beingWrittenOutputStream.keySet().stream().forEach(file -> {
            try {
                boolean deleted = HdfsUtils.deleteFile(file);
                if (!deleted) {
                    LOGGER.error("delete file {} error", file);
                    throw new IOException(String.format("delete file {} error", file));
                }
            } catch (IOException e) {
                LOGGER.error("delete file {} error", file);
                throw new RuntimeException(e);
            }
        });

        this.beingWrittenOutputStream = new HashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        String line = transformRowToLine(seaTunnelRow);
        try {
            fsDataOutputStream.write(line.getBytes());
            fsDataOutputStream.write(hiveSinkConfig.getHiveTxtFileLineDelimiter().getBytes());
        } catch (IOException e) {
            LOGGER.error("write data to file {} error", filePath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getNeedMoveFiles() {
        return this.needMoveFiles;
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

            needMoveFiles.put(entry.getKey(), getHiveLocation(entry.getKey()));
        });
    }

    private FSDataOutputStream getOrCreateOutputStream(String filePath) {
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

    private String transformRowToLine(SeaTunnelRow seaTunnelRow) {
        String line = null;
        List<String> sinkColumns = hiveSinkConfig.getSinkColumns();
        if (sinkColumns == null || sinkColumns.size() == 0) {
            line = Arrays.stream(seaTunnelRow.getFields())
                .map(column -> column == null ? "" : column.toString())
                .collect(Collectors.joining(hiveSinkConfig.getHiveTxtFileFieldDelimiter()));
        } else {
            line = sinkColumns.stream().map(column -> {
                String valueStr = "";
                Object value = seaTunnelRow.getFieldMap().get(column);
                if (value != null) {
                    valueStr = value.toString();
                }
                return valueStr;
            }).collect(Collectors.joining(hiveSinkConfig.getHiveTxtFileFieldDelimiter()));
        }
        return line;
    }
}

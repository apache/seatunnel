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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.ArchiveCompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class JsonReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private CompressFormat compressFormat = FileBaseSourceOptions.COMPRESS_CODEC.defaultValue();
    private String encoding = FileBaseSourceOptions.ENCODING.defaultValue();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(FileBaseSourceOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(FileBaseSourceOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(FileBaseSourceOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        super.setCatalogTable(catalogTable);
        if (isMergePartition) {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.seaTunnelRowTypeWithPartition);
        } else {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.seaTunnelRowType);
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.JSON);
    }

    @Override
    public void readSplit(
            String filePath,
            String tableId,
            Collector<SeaTunnelRow> output,
            long startOffset,
            long length,
            boolean isFirstSplit)
            throws IOException, FileConnectorException {
        Map<String, String> partitionsMap = parsePartitionsByPath(filePath);
        if (compressFormat != CompressFormat.NONE
                || archiveCompressFormat != ArchiveCompressFormat.NONE) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED,
                    String.format(
                            "Compressed/archived JSON files do not support file splitting. File: %s, compress: %s, archive: %s",
                            filePath, compressFormat, archiveCompressFormat));
        }
        readSplitProcess(
                filePath, tableId, output, startOffset, length, isFirstSplit, partitionsMap);
    }

    private void readSplitProcess(
            String filePath,
            String tableId,
            Collector<SeaTunnelRow> output,
            long startOffset,
            long length,
            boolean isFirstSplit,
            Map<String, String> partitionsMap)
            throws IOException, FileConnectorException {

        try (InputStream inputStream = hadoopFileSystemProxy.getInputStream(filePath)) {
            // Skip to start offset if not reading from beginning
            if (startOffset > 0) {
                long skipped = inputStream.skip(startOffset);
                if (skipped != startOffset) {
                    throw new IOException(
                            String.format(
                                    "Failed to skip to offset %d, actually skipped %d",
                                    startOffset, skipped));
                }
            }

            // Create bounded input stream if length is specified
            InputStream boundedStream =
                    (length > 0) ? new BoundedInputStream(inputStream, length) : inputStream;

            InputStream actualInputStream;
            switch (compressFormat) {
                case LZO:
                    LzopCodec lzo = new LzopCodec();
                    actualInputStream = lzo.createInputStream(boundedStream);
                    break;
                case NONE:
                    actualInputStream = boundedStream;
                    break;
                default:
                    throw new FileConnectorException(
                            FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                            String.format(
                                    "Unsupported json compress codec: %s",
                                    compressFormat.getCompressCodec()));
            }

            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(actualInputStream, encoding))) {

                // For non-first splits, skip potential partial first line
                if (!isFirstSplit) {
                    reader.readLine(); // Skip the potentially incomplete first line
                }

                reader.lines()
                        .forEach(
                                line -> {
                                    try {
                                        SeaTunnelRow seaTunnelRow =
                                                deserializationSchema.deserialize(
                                                        line.getBytes(StandardCharsets.UTF_8));
                                        if (isMergePartition) {
                                            int index = seaTunnelRowType.getTotalFields();
                                            for (String value : partitionsMap.values()) {
                                                seaTunnelRow.setField(index++, value);
                                            }
                                        }
                                        seaTunnelRow.setTableId(tableId);
                                        output.collect(seaTunnelRow);
                                    } catch (IOException e) {
                                        String errorMsg =
                                                String.format(
                                                        "Deserialize this jsonFile data [%s] failed, please check the origin data",
                                                        line);
                                        throw new FileConnectorException(
                                                FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                                                errorMsg,
                                                e);
                                    }
                                });
            }
        }
    }

    @Override
    public void readProcess(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {
        InputStream actualInputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                actualInputStream = lzo.createInputStream(inputStream);
                break;
            case NONE:
                actualInputStream = inputStream;
                break;
            default:
                throw new FileConnectorException(
                        FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                        String.format(
                                "Unsupported json compress codec: %s",
                                compressFormat.getCompressCodec()));
        }
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(actualInputStream, encoding))) {
            reader.lines()
                    .forEach(
                            line -> {
                                try {
                                    SeaTunnelRow seaTunnelRow =
                                            deserializationSchema.deserialize(
                                                    line.getBytes(StandardCharsets.UTF_8));
                                    if (isMergePartition) {
                                        int index = seaTunnelRowType.getTotalFields();
                                        for (String value : partitionsMap.values()) {
                                            seaTunnelRow.setField(index++, value);
                                        }
                                    }
                                    seaTunnelRow.setTableId(tableId);
                                    output.collect(seaTunnelRow);
                                } catch (IOException e) {
                                    String errorMsg =
                                            String.format(
                                                    "Deserialize this jsonFile data [%s] failed, please check the origin data",
                                                    line);
                                    throw new FileConnectorException(
                                            FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                                            errorMsg,
                                            e);
                                }
                            });
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }
}

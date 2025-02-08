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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;
import org.apache.seatunnel.format.text.splitor.DefaultTextLineSplitor;
import org.apache.seatunnel.format.text.splitor.TextLineSplitor;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TextReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private String fieldDelimiter = BaseSourceConfigOptions.FIELD_DELIMITER.defaultValue();
    private DateUtils.Formatter dateFormat = BaseSourceConfigOptions.DATE_FORMAT.defaultValue();
    private DateTimeUtils.Formatter datetimeFormat =
            BaseSourceConfigOptions.DATETIME_FORMAT.defaultValue();
    private TimeUtils.Formatter timeFormat = BaseSourceConfigOptions.TIME_FORMAT.defaultValue();
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();
    private TextLineSplitor textLineSplitor;
    private int[] indexes;
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.TEXT);
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
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                actualInputStream = inputStream;
                break;
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(actualInputStream, encoding))) {
            reader.lines()
                    .skip(skipHeaderNumber)
                    .forEach(
                            line -> {
                                try {
                                    SeaTunnelRow seaTunnelRow =
                                            deserializationSchema.deserialize(
                                                    line.getBytes(StandardCharsets.UTF_8));
                                    if (!readColumns.isEmpty()) {
                                        // need column projection
                                        Object[] fields;
                                        if (isMergePartition) {
                                            fields =
                                                    new Object
                                                            [readColumns.size()
                                                                    + partitionsMap.size()];
                                        } else {
                                            fields = new Object[readColumns.size()];
                                        }
                                        for (int i = 0; i < indexes.length; i++) {
                                            fields[i] = seaTunnelRow.getField(indexes[i]);
                                        }
                                        seaTunnelRow = new SeaTunnelRow(fields);
                                    }
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
                                                    "Deserialize this data [%s] failed, please check the origin data",
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
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) {
        this.seaTunnelRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        initFormatter();
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading text files, if user has not specified schema information, "
                            + "SeaTunnel will not support column projection");
        }
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(TextFormatConstant.PLACEHOLDER)
                        .textLineSplitor(textLineSplitor)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(BaseSourceConfigOptions.NULL_FORMAT)
                                        .orElse(null));
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(this.seaTunnelRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(this.seaTunnelRowType).build();
        }
        return getActualSeaTunnelRowTypeInfo();
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), rowType);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        Optional<String> fieldDelimiterOptional =
                readonlyConfig.getOptional(BaseSourceConfigOptions.FIELD_DELIMITER);
        encoding =
                readonlyConfig
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        fieldDelimiterOptional.ifPresent(s -> fieldDelimiter = s);
        initFormatter();
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(fieldDelimiter)
                        .textLineSplitor(textLineSplitor)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(BaseSourceConfigOptions.NULL_FORMAT)
                                        .orElse(null));
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(userDefinedRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(rowType).build();
        }
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = rowType.indexOf(readColumns.get(i));
                fields[i] = rowType.getFieldName(indexes[i]);
                types[i] = rowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = rowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    private void initFormatter() {
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        textLineSplitor = new DefaultTextLineSplitor();
    }
}

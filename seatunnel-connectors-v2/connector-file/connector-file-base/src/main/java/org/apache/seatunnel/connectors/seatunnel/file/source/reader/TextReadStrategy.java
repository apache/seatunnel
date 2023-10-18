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
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class TextReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private String fieldDelimiter = BaseSourceConfig.DELIMITER.defaultValue();
    private DateUtils.Formatter dateFormat = BaseSourceConfig.DATE_FORMAT.defaultValue();
    private DateTimeUtils.Formatter datetimeFormat =
            BaseSourceConfig.DATETIME_FORMAT.defaultValue();
    private TimeUtils.Formatter timeFormat = BaseSourceConfig.TIME_FORMAT.defaultValue();
    private CompressFormat compressFormat = BaseSourceConfig.COMPRESS_CODEC.defaultValue();
    private int[] indexes;

    @Override
    public void read(String path, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        InputStream inputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(fs.open(filePath));
                break;
            case NONE:
                inputStream = fs.open(filePath);
                break;
            default:
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = fs.open(filePath);
                break;
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            reader.lines()
                    .skip(skipHeaderNumber)
                    .forEach(
                            line -> {
                                try {
                                    SeaTunnelRow seaTunnelRow =
                                            deserializationSchema.deserialize(line.getBytes());
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
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) {
        this.seaTunnelRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        initFormatter();
        if (pluginConfig.hasPath(BaseSourceConfig.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading json/text/csv files, if user has not specified schema information, "
                            + "SeaTunnel will not support column projection");
        }
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(TextFormatConstant.PLACEHOLDER)
                        .dateFormatter(dateFormat)
                        .dateTimeFormatter(datetimeFormat)
                        .timeFormatter(timeFormat);
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(this.seaTunnelRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(this.seaTunnelRowType).build();
        }
        return getActualSeaTunnelRowTypeInfo();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        if (pluginConfig.hasPath(BaseSourceConfig.DELIMITER.key())) {
            fieldDelimiter = pluginConfig.getString(BaseSourceConfig.DELIMITER.key());
        } else {
            FileFormat fileFormat =
                    FileFormat.valueOf(
                            pluginConfig
                                    .getString(BaseSourceConfig.FILE_FORMAT_TYPE.key())
                                    .toUpperCase());
            if (fileFormat == FileFormat.CSV) {
                fieldDelimiter = ",";
            }
        }
        initFormatter();
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(fieldDelimiter)
                        .dateFormatter(dateFormat)
                        .dateTimeFormatter(datetimeFormat)
                        .timeFormatter(timeFormat);
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(userDefinedRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(seaTunnelRowType).build();
        }
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfig.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = seaTunnelRowType.indexOf(readColumns.get(i));
                fields[i] = seaTunnelRowType.getFieldName(indexes[i]);
                types[i] = seaTunnelRowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = seaTunnelRowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    private void initFormatter() {
        if (pluginConfig.hasPath(BaseSourceConfig.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfig.DATE_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfig.DATETIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfig.TIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.COMPRESS_CODEC.key())) {
            String compressCodec = pluginConfig.getString(BaseSourceConfig.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
    }
}

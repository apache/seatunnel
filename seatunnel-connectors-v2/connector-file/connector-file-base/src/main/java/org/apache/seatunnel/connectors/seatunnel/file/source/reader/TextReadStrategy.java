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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TextReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private String fieldDelimiter = String.valueOf('\001');
    private DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    private DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    private TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws FileConnectorException, IOException {
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath), StandardCharsets.UTF_8))) {
            reader.lines().forEach(line -> {
                try {
                    SeaTunnelRow seaTunnelRow = deserializationSchema.deserialize(line.getBytes());
                    if (isMergePartition) {
                        int index = seaTunnelRowType.getTotalFields();
                        for (String value : partitionsMap.values()) {
                            seaTunnelRow.setField(index++, value);
                        }
                    }
                    output.collect(seaTunnelRow);
                } catch (IOException e) {
                    String errorMsg = String.format("Deserialize this data [%s] failed, please check the origin data", line);
                    throw new FileConnectorException(FileConnectorErrorCode.DATA_DESERIALIZE_FAILED, errorMsg, e);
                }
            });
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) {
        SeaTunnelRowType simpleSeaTunnelType = SeaTunnelSchema.buildSimpleTextSchema();
        this.seaTunnelRowType = simpleSeaTunnelType;
        this.seaTunnelRowTypeWithPartition = mergePartitionTypes(fileNames.get(0), simpleSeaTunnelType);
        if (isMergePartition) {
            deserializationSchema = TextDeserializationSchema.builder()
                    .seaTunnelRowType(this.seaTunnelRowTypeWithPartition)
                    .delimiter(String.valueOf('\002'))
                    .build();
        } else {
            deserializationSchema = TextDeserializationSchema.builder()
                    .seaTunnelRowType(this.seaTunnelRowType)
                    .delimiter(String.valueOf('\002'))
                    .build();
        }
        return getActualSeaTunnelRowTypeInfo();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        if (pluginConfig.hasPath(BaseSourceConfig.DELIMITER.key())) {
            fieldDelimiter = pluginConfig.getString(BaseSourceConfig.DELIMITER.key());
        } else {
            FileFormat fileFormat = FileFormat.valueOf(pluginConfig.getString(BaseSourceConfig.FILE_TYPE.key()).toUpperCase());
            if (fileFormat == FileFormat.CSV) {
                fieldDelimiter = ",";
            }
        }
        if (pluginConfig.hasPath(BaseSourceConfig.DATE_FORMAT.key())) {
            dateFormat = DateUtils.Formatter.parse(pluginConfig.getString(BaseSourceConfig.DATE_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.DATETIME_FORMAT.key())) {
            datetimeFormat = DateTimeUtils.Formatter.parse(pluginConfig.getString(BaseSourceConfig.DATETIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfig.TIME_FORMAT.key())) {
            timeFormat = TimeUtils.Formatter.parse(pluginConfig.getString(BaseSourceConfig.TIME_FORMAT.key()));
        }
        if (isMergePartition) {
            deserializationSchema = TextDeserializationSchema.builder()
                    .seaTunnelRowType(this.seaTunnelRowTypeWithPartition)
                    .delimiter(fieldDelimiter)
                    .dateFormatter(dateFormat)
                    .dateTimeFormatter(datetimeFormat)
                    .timeFormatter(timeFormat)
                    .build();
        } else {
            deserializationSchema = TextDeserializationSchema.builder()
                    .seaTunnelRowType(this.seaTunnelRowType)
                    .delimiter(fieldDelimiter)
                    .dateFormatter(dateFormat)
                    .dateTimeFormatter(datetimeFormat)
                    .timeFormatter(timeFormat)
                    .build();
        }
    }
}

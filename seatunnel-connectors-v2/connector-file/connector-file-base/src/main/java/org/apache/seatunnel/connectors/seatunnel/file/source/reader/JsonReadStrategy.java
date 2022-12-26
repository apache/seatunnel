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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        if (isMergePartition) {
            deserializationSchema = new JsonDeserializationSchema(false, false, this.seaTunnelRowTypeWithPartition);
        } else {
            deserializationSchema = new JsonDeserializationSchema(false, false, this.seaTunnelRowType);
        }
    }

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
                    String errorMsg = String.format("Read data from this file [%s] failed", filePath);
                    throw new FileConnectorException(CommonErrorCode.FILE_OPERATION_FAILED, errorMsg);
                }
            });
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) throws FileConnectorException {
        throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }
}

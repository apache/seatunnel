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

package org.apache.seatunnel.connectors.seatunnel.hudi.util;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode.TABLE_CONFIG_NOT_FOUND;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;

@Slf4j
public class HudiWriteClientUtil implements Serializable {

    public static HoodieJavaWriteClient<HoodieAvroPayload> createHoodieJavaWriteClient(
            HudiSinkConfig hudiSinkConfig, SeaTunnelRowType seaTunnelRowType, String tableName) {
        List<HudiTableConfig> tableList = hudiSinkConfig.getTableList();
        Optional<HudiTableConfig> hudiTableConfig =
                tableList.stream()
                        .filter(table -> table.getTableName().equals(tableName))
                        .findFirst();
        if (!hudiTableConfig.isPresent()) {
            throw new HudiConnectorException(
                    TABLE_CONFIG_NOT_FOUND,
                    "The corresponding table "
                            + tableName
                            + " is not found in the table list of hudi sink config.");
        }
        Configuration hadoopConf;
        if (hudiSinkConfig.getConfFilesPath() != null) {
            hadoopConf = HudiUtil.getConfiguration(hudiSinkConfig.getConfFilesPath());
        } else {
            hadoopConf = new Configuration();
        }

        HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder();
        // build index config
        if (Objects.nonNull(hudiTableConfig.get().getIndexClassName())) {
            writeConfigBuilder.withIndexConfig(
                    HoodieIndexConfig.newBuilder()
                            .withIndexClass(hudiTableConfig.get().getIndexClassName())
                            .build());
        } else {
            writeConfigBuilder.withIndexConfig(
                    HoodieIndexConfig.newBuilder()
                            .withIndexType(hudiTableConfig.get().getIndexType())
                            .build());
        }
        HoodieWriteConfig cfg =
                writeConfigBuilder
                        .withEngineType(EngineType.JAVA)
                        .withPath(hudiTableConfig.get().getTableDfsPath())
                        .withSchema(convertToSchema(seaTunnelRowType).toString())
                        .withParallelism(
                                hudiSinkConfig.getInsertShuffleParallelism(),
                                hudiSinkConfig.getUpsertShuffleParallelism())
                        .forTable(hudiTableConfig.get().getTableName())
                        .withArchivalConfig(
                                HoodieArchivalConfig.newBuilder()
                                        .archiveCommitsWith(
                                                hudiSinkConfig.getMinCommitsToKeep(),
                                                hudiSinkConfig.getMaxCommitsToKeep())
                                        .build())
                        .withAutoCommit(hudiSinkConfig.isAutoCommit())
                        .withCleanConfig(
                                HoodieCleanConfig.newBuilder()
                                        .withAutoClean(true)
                                        .withAsyncClean(false)
                                        .build())
                        .withEmbeddedTimelineServerEnabled(false)
                        .withCompactionConfig(
                                HoodieCompactionConfig.newBuilder()
                                        .approxRecordSize(hudiTableConfig.get().getRecordByteSize())
                                        .build())
                        .withStorageConfig(
                                HoodieStorageConfig.newBuilder()
                                        .parquetCompressionCodec(CompressionCodecName.SNAPPY.name())
                                        .build())
                        .build();
        return new HoodieJavaWriteClient<>(
                new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf)), cfg);
    }
}

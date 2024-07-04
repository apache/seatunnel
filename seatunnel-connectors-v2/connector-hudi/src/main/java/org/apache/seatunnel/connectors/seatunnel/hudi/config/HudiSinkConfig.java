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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder(builderClassName = "Builder")
public class HudiSinkConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tableName;

    private String tableDfsPath;

    private int insertShuffleParallelism;

    private int upsertShuffleParallelism;

    private int minCommitsToKeep;

    private int maxCommitsToKeep;

    private HoodieTableType tableType;

    private WriteOperationType opType;

    private String confFilesPath;

    private int batchIntervalMs;

    private String recordKeyFields;

    private String partitionFields;

    public static HudiSinkConfig of(ReadonlyConfig config) {
        HudiSinkConfig.Builder builder = HudiSinkConfig.builder();
        builder.confFilesPath(config.get(HudiOptions.CONF_FILES_PATH));
        builder.tableName(config.get(HudiOptions.TABLE_NAME));
        builder.tableDfsPath(config.get(HudiOptions.TABLE_DFS_PATH));
        builder.tableType(config.get(HudiOptions.TABLE_TYPE));
        builder.opType(config.get(HudiOptions.OP_TYPE));

        builder.batchIntervalMs(config.get(HudiOptions.BATCH_INTERVAL_MS));

        builder.partitionFields(config.get(HudiOptions.PARTITION_FIELDS));

        builder.recordKeyFields(config.get(HudiOptions.RECORD_KEY_FIELDS));

        builder.insertShuffleParallelism(config.get(HudiOptions.INSERT_SHUFFLE_PARALLELISM));

        builder.upsertShuffleParallelism(config.get(HudiOptions.UPSERT_SHUFFLE_PARALLELISM));

        builder.minCommitsToKeep(config.get(HudiOptions.MIN_COMMITS_TO_KEEP));
        builder.maxCommitsToKeep(config.get(HudiOptions.MAX_COMMITS_TO_KEEP));
        return builder.build();
    }
}

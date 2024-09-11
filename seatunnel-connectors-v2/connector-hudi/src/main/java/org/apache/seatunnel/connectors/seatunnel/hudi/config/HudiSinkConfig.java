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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import org.apache.hudi.common.model.WriteOperationType;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class HudiSinkConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tableDfsPath;

    private List<HudiTableConfig> tableList;

    private WriteOperationType opType;

    private int insertShuffleParallelism;

    private int upsertShuffleParallelism;

    private int minCommitsToKeep;

    private int maxCommitsToKeep;

    private String confFilesPath;

    private int batchIntervalMs;

    private int batchSize;

    private boolean autoCommit;

    private SchemaSaveMode schemaSaveMode;

    private DataSaveMode dataSaveMode;

    public static HudiSinkConfig of(ReadonlyConfig config) {
        Builder builder = HudiSinkConfig.builder();
        Optional<Integer> optionalInsertShuffleParallelism =
                config.getOptional(HudiOptions.INSERT_SHUFFLE_PARALLELISM);
        Optional<Integer> optionalUpsertShuffleParallelism =
                config.getOptional(HudiOptions.UPSERT_SHUFFLE_PARALLELISM);
        Optional<Integer> optionalMinCommitsToKeep =
                config.getOptional(HudiOptions.MIN_COMMITS_TO_KEEP);
        Optional<Integer> optionalMaxCommitsToKeep =
                config.getOptional(HudiOptions.MAX_COMMITS_TO_KEEP);

        Optional<WriteOperationType> optionalOpType = config.getOptional(HudiOptions.OP_TYPE);

        Optional<Integer> optionalBatchIntervalMs =
                config.getOptional(HudiOptions.BATCH_INTERVAL_MS);
        Optional<Integer> optionalBatchSize = config.getOptional(HudiOptions.BATCH_SIZE);
        Optional<Boolean> optionalAutoCommit = config.getOptional(HudiOptions.AUTO_COMMIT);
        Optional<SchemaSaveMode> optionalSchemaSaveMode =
                config.getOptional(HudiOptions.SCHEMA_SAVE_MODE);
        Optional<DataSaveMode> optionalDataSaveMode =
                config.getOptional(HudiOptions.DATA_SAVE_MODE);

        builder.tableDfsPath(config.get(HudiOptions.TABLE_DFS_PATH));
        builder.confFilesPath(config.get(HudiOptions.CONF_FILES_PATH));
        builder.tableList(HudiTableConfig.of(config));
        builder.opType(optionalOpType.orElseGet(HudiOptions.OP_TYPE::defaultValue));

        builder.batchIntervalMs(
                optionalBatchIntervalMs.orElseGet(HudiOptions.BATCH_INTERVAL_MS::defaultValue));
        builder.batchSize(optionalBatchSize.orElseGet(HudiOptions.BATCH_SIZE::defaultValue));

        builder.insertShuffleParallelism(
                optionalInsertShuffleParallelism.orElseGet(
                        HudiOptions.INSERT_SHUFFLE_PARALLELISM::defaultValue));
        builder.upsertShuffleParallelism(
                optionalUpsertShuffleParallelism.orElseGet(
                        HudiOptions.UPSERT_SHUFFLE_PARALLELISM::defaultValue));

        builder.minCommitsToKeep(
                optionalMinCommitsToKeep.orElseGet(HudiOptions.MIN_COMMITS_TO_KEEP::defaultValue));
        builder.maxCommitsToKeep(
                optionalMaxCommitsToKeep.orElseGet(HudiOptions.MAX_COMMITS_TO_KEEP::defaultValue));
        builder.autoCommit(optionalAutoCommit.orElseGet(HudiOptions.AUTO_COMMIT::defaultValue));
        builder.schemaSaveMode(
                optionalSchemaSaveMode.orElseGet(HudiOptions.SCHEMA_SAVE_MODE::defaultValue));
        builder.dataSaveMode(
                optionalDataSaveMode.orElseGet(HudiOptions.DATA_SAVE_MODE::defaultValue));
        return builder.build();
    }
}

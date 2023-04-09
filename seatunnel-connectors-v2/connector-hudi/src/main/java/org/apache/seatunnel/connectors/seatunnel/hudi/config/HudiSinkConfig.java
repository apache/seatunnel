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
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class HudiSinkConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tableName;

    private String tablePath;

    private int insertShuffleParallelism;

    private int upsertShuffleParallelism;

    private int deleteShuffleParallelism;

    private int minCommitsToKeep;

    private int maxCommitsToKeep;

    private HoodieTableType tableType;

    private WriteOperationType opType;

    private String confFile;

    private int batchIntervalMs;

    private String recordKeyFields;

    private String partitionFields;

    public static HudiSinkConfig of(ReadonlyConfig config) {
        HudiSinkConfig.Builder builder = HudiSinkConfig.builder();
        Optional<Integer> optionalInsertShuffleParallelism =
                config.getOptional(HudiOptions.INSERT_SHUFFLE_PARALLELISM);
        Optional<Integer> optionalUpsertShuffleParallelism =
                config.getOptional(HudiOptions.UPSERT_SHUFFLE_PARALLELISM);
        Optional<Integer> optionalDeleteShuffleParallelism =
                config.getOptional(HudiOptions.DELETE_SHUFFLE_PARALLELISM);
        Optional<Integer> optionalMinCommitsToKeep =
                config.getOptional(HudiOptions.MIN_COMMITS_TO_KEEP);
        Optional<Integer> optionalMaxCommitsToKeep =
                config.getOptional(HudiOptions.MAX_COMMITS_TO_KEEP);

        Optional<HoodieTableType> optionalTableType = config.getOptional(HudiOptions.TABLE_TYPE);
        Optional<WriteOperationType> optionalOpType = config.getOptional(HudiOptions.OP_TYPE);

        Optional<Integer> optionalBatchIntervalMs =
                config.getOptional(HudiOptions.BATCH_INTERVAL_MS);

        Optional<String> partitionFields = config.getOptional(HudiOptions.PARTITION_FIELDS);

        Optional<String> recordKeyFields = config.getOptional(HudiOptions.RECORD_KEY_FIELDS);

        builder.confFile(config.get(HudiOptions.CONF_FILES));
        builder.tableName(config.get(HudiOptions.TABLE_NAME));
        builder.tablePath(config.get(HudiOptions.TABLE_PATH));
        builder.tableType(optionalTableType.orElseGet(HudiOptions.TABLE_TYPE::defaultValue));
        builder.opType(optionalOpType.orElseGet(HudiOptions.OP_TYPE::defaultValue));

        builder.batchIntervalMs(
                optionalBatchIntervalMs.orElseGet(HudiOptions.BATCH_INTERVAL_MS::defaultValue));

        builder.partitionFields(
                partitionFields.orElseGet(HudiOptions.PARTITION_FIELDS::defaultValue));

        builder.recordKeyFields(
                recordKeyFields.orElseGet(HudiOptions.RECORD_KEY_FIELDS::defaultValue));

        builder.insertShuffleParallelism(
                optionalInsertShuffleParallelism.orElseGet(
                        HudiOptions.INSERT_SHUFFLE_PARALLELISM::defaultValue));
        builder.upsertShuffleParallelism(
                optionalUpsertShuffleParallelism.orElseGet(
                        HudiOptions.UPSERT_SHUFFLE_PARALLELISM::defaultValue));
        builder.deleteShuffleParallelism(
                optionalDeleteShuffleParallelism.orElseGet(
                        HudiOptions.DELETE_SHUFFLE_PARALLELISM::defaultValue));

        builder.minCommitsToKeep(
                optionalMinCommitsToKeep.orElseGet(HudiOptions.MIN_COMMITS_TO_KEEP::defaultValue));
        builder.maxCommitsToKeep(
                optionalMaxCommitsToKeep.orElseGet(HudiOptions.MAX_COMMITS_TO_KEEP::defaultValue));
        return builder.build();
    }
}

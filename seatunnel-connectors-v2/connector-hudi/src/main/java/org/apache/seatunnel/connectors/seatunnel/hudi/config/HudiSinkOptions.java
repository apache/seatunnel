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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.index.HoodieIndex;

import java.util.List;

public class HudiSinkOptions {

    public static Option<String> TABLE_DFS_PATH =
            Options.key("table_dfs_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the dfs path of hudi table");

    public static Option<String> CONF_FILES_PATH =
            Options.key("conf_files_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi conf files");

    public static Option<List<HudiTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(HudiTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table_list");

    public static Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema save mode");

    public static Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data save mode");

    public static Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi table name");

    public static Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("hudi database name");

    public static Option<HoodieTableType> TABLE_TYPE =
            Options.key("table_type")
                    .type(new TypeReference<HoodieTableType>() {})
                    .defaultValue(HoodieTableType.COPY_ON_WRITE)
                    .withDescription("hudi table type");

    public static Option<Boolean> CDC_ENABLED =
            Options.key("cdc_enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When enable, persist the change data if necessary, and can be queried as a CDC query mode.");

    public static Option<String> RECORD_KEY_FIELDS =
            Options.key("record_key_fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the record key fields of hudi table");

    public static Option<String> PARTITION_FIELDS =
            Options.key("partition_fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the partition fields of hudi table");

    public static Option<HoodieIndex.IndexType> INDEX_TYPE =
            Options.key("index_type")
                    .type(new TypeReference<HoodieIndex.IndexType>() {})
                    .defaultValue(HoodieIndex.IndexType.BLOOM)
                    .withDescription(
                            "the index type of hudi table, currently supported: [BLOOM, SIMPLE, GLOBAL_BLOOM]");

    public static Option<String> INDEX_CLASS_NAME =
            Options.key("index_class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "customized hudi index type, the index classpath is configured here");

    public static Option<Integer> RECORD_BYTE_SIZE =
            Options.key("record_byte_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The byte size of each record");

    public static Option<WriteOperationType> OP_TYPE =
            Options.key("op_type")
                    .type(new TypeReference<WriteOperationType>() {})
                    .defaultValue(WriteOperationType.INSERT)
                    .withDescription("op_type");

    public static Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("the size of each insert batch");

    public static Option<Integer> BATCH_INTERVAL_MS =
            Options.key("batch_interval_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("batch interval milliSecond");

    public static Option<Integer> INSERT_SHUFFLE_PARALLELISM =
            Options.key("insert_shuffle_parallelism")
                    .intType()
                    .defaultValue(2)
                    .withDescription("insert_shuffle_parallelism");

    public static Option<Integer> UPSERT_SHUFFLE_PARALLELISM =
            Options.key("upsert_shuffle_parallelism")
                    .intType()
                    .defaultValue(2)
                    .withDescription("upsert_shuffle_parallelism");

    public static Option<Integer> MIN_COMMITS_TO_KEEP =
            Options.key("min_commits_to_keep")
                    .intType()
                    .defaultValue(20)
                    .withDescription("hoodie.keep.min.commits");

    public static Option<Integer> MAX_COMMITS_TO_KEEP =
            Options.key("max_commits_to_keep")
                    .intType()
                    .defaultValue(30)
                    .withDescription("hoodie.keep.max.commits");
}

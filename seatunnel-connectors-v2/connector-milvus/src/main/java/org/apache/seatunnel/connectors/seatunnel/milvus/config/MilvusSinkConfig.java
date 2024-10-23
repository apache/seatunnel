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

package org.apache.seatunnel.connectors.seatunnel.milvus.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;

public class MilvusSinkConfig extends MilvusCommonConfig {

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");
    public static final Option<Map<String, String>> COLLECTION_DESCRIPTION =
            Options.key("collection_description")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("collection description");
    public static final Option<String> PARTITION_KEY =
            Options.key("partition_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus partition key field");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .singleChoice(
                            DataSaveMode.class,
                            Arrays.asList(DROP_DATA, APPEND_DATA, ERROR_WHEN_DATA_EXISTS))
                    .defaultValue(APPEND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<Boolean> ENABLE_AUTO_ID =
            Options.key("enable_auto_id")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable Auto Id");

    public static final Option<Boolean> ENABLE_UPSERT =
            Options.key("enable_upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable upsert mode");

    public static final Option<Boolean> ENABLE_DYNAMIC_FIELD =
            Options.key("enable_dynamic_field")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable dynamic field");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("writer batch size");
    public static final Option<Integer> RATE_LIMIT =
            Options.key("rate_limit")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("writer rate limit");
    public static final Option<Boolean> LOAD_COLLECTION =
            Options.key("load_collection")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if load collection");
    public static final Option<Boolean> CREATE_INDEX =
            Options.key("create_index")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("if load collection");
}

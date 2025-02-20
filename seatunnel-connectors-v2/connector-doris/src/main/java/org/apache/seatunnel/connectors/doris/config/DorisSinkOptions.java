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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Map;

import static org.apache.seatunnel.api.options.SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_BATCH_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.FENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.PASSWORD;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE_IDENTIFIER;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.USERNAME;

public interface DorisSinkOptions {

    int DEFAULT_SINK_CHECK_INTERVAL = 10000;
    int DEFAULT_SINK_MAX_RETRIES = 3;
    int DEFAULT_SINK_BUFFER_SIZE = 256 * 1024;
    int DEFAULT_SINK_BUFFER_COUNT = 3;

    Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("enable 2PC while loading");

    Option<Integer> SINK_CHECK_INTERVAL =
            Options.key("sink.check-interval")
                    .intType()
                    .defaultValue(DEFAULT_SINK_CHECK_INTERVAL)
                    .withDescription("check exception with the interval while loading");
    Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(DEFAULT_SINK_MAX_RETRIES)
                    .withDescription("the max retry times if writing records to database failed.");
    Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_SIZE)
                    .withDescription("the buffer size to cache data for stream load.");
    Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_COUNT)
                    .withDescription("the buffer count to cache data for stream load.");
    Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .defaultValue("")
                    .withDescription("the unique label prefix.");
    Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable the delete function");

    Option<Map<String, String>> DORIS_SINK_CONFIG_PREFIX =
            Options.key("doris.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the Stream Load data_desc. "
                                    + "The way to specify the parameter is to add the prefix `doris.config` to the original load parameter name ");

    Option<String> DEFAULT_DATABASE =
            Options.key("default-database")
                    .stringType()
                    .defaultValue("information_schema")
                    .withDescription("");

    Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data_save_mode");

    Option<String> CUSTOM_SQL =
            Options.key("custom_sql").stringType().noDefaultValue().withDescription("custom_sql");

    Option<Boolean> NEEDS_UNSUPPORTED_TYPE_CASTING =
            Options.key("needs_unsupported_type_casting")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable the unsupported type casting, such as Decimal64 to Double");

    // create table
    Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `"
                                    + SaveModePlaceHolder.DATABASE.getPlaceHolder()
                                    + "`.`"
                                    + SaveModePlaceHolder.TABLE.getPlaceHolder()
                                    + "` (\n"
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ",\n"
                                    + SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder()
                                    + "\n"
                                    + ") ENGINE=OLAP\n"
                                    + " UNIQUE KEY ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")\n"
                                    + "COMMENT '"
                                    + SaveModePlaceHolder.COMMENT.getPlaceHolder()
                                    + "'\n"
                                    + "DISTRIBUTED BY HASH ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")\n "
                                    + "PROPERTIES (\n"
                                    + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                                    + "\"in_memory\" = \"false\",\n"
                                    + "\"storage_format\" = \"V2\",\n"
                                    + "\"disable_auto_compaction\" = \"false\"\n"
                                    + ")")
                    .withDescription("Create table statement template, used to create Doris table");

    OptionRule.Builder SINK_RULE =
            OptionRule.builder()
                    .required(
                            FENODES,
                            USERNAME,
                            PASSWORD,
                            SINK_LABEL_PREFIX,
                            DORIS_SINK_CONFIG_PREFIX,
                            DATA_SAVE_MODE,
                            SCHEMA_SAVE_MODE)
                    .optional(
                            DATABASE,
                            TABLE,
                            TABLE_IDENTIFIER,
                            QUERY_PORT,
                            DORIS_BATCH_SIZE,
                            SINK_ENABLE_2PC,
                            SINK_ENABLE_DELETE,
                            MULTI_TABLE_SINK_REPLICA,
                            SAVE_MODE_CREATE_TEMPLATE,
                            NEEDS_UNSUPPORTED_TYPE_CASTING)
                    .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL);
}

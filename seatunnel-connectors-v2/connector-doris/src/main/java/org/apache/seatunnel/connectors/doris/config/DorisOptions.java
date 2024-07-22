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

import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;

public interface DorisOptions {

    int DORIS_TABLET_SIZE_MIN = 1;
    int DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;
    int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;
    int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;
    int DORIS_BATCH_SIZE_DEFAULT = 1024;
    long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;
    int DEFAULT_SINK_CHECK_INTERVAL = 10000;
    int DEFAULT_SINK_MAX_RETRIES = 3;
    int DEFAULT_SINK_BUFFER_SIZE = 256 * 1024;
    int DEFAULT_SINK_BUFFER_COUNT = 3;

    String DORIS_DEFAULT_CLUSTER = "default_cluster";

    // common option
    Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris fe http address.");

    Option<Integer> QUERY_PORT =
            Options.key("query-port")
                    .intType()
                    .defaultValue(9030)
                    .withDescription("doris query port");

    @Deprecated
    Option<String> TABLE_IDENTIFIER =
            Options.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris table name.");

    Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris user name.");
    Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris password.");

    Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris table name.");
    Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris database name.");
    Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("the batch size of the doris read/write.");

    // source config options
    Option<String> DORIS_READ_FIELD =
            Options.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names in the Doris table, separated by commas");
    Option<String> DORIS_FILTER_QUERY =
            Options.key("doris.filter.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");
    Option<Integer> DORIS_TABLET_SIZE =
            Options.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
                    .withDescription("");
    Option<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS =
            Options.key("doris.request.connect.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");
    Option<Integer> DORIS_REQUEST_READ_TIMEOUT_MS =
            Options.key("doris.request.read.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");
    Option<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S =
            Options.key("doris.request.query.timeout.s")
                    .intType()
                    .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");
    Option<Integer> DORIS_REQUEST_RETRIES =
            Options.key("doris.request.retries")
                    .intType()
                    .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");
    Option<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC =
            Options.key("doris.deserialize.arrow.async")
                    .booleanType()
                    .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");
    Option<Integer> DORIS_DESERIALIZE_QUEUE_SIZE =
            Options.key("doris.request.retriesdoris.deserialize.queue.size")
                    .intType()
                    .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");

    Option<Long> DORIS_EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");

    // sink config options
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

    OptionRule.Builder CATALOG_RULE =
            OptionRule.builder().required(FENODES, QUERY_PORT, USERNAME, PASSWORD);
}

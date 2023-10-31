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

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableMap;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;

import java.util.Collections;
import java.util.Map;

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

    Map<String, String> DEFAULT_CREATE_PROPERTIES =
            ImmutableMap.of(
                    "replication_allocation", "tag.location.default: 3",
                    "storage_format", "V2",
                    "disable_auto_compaction", "false");

    String DEFAULT_CREATE_TEMPLATE =
            "CREATE TABLE ${table_identifier}\n"
                    + "(\n"
                    + "${column_definition}\n"
                    + ")\n"
                    + "ENGINE = ${engine_type}\n"
                    + "UNIQUE KEY (${key_columns})\n"
                    + "COMMENT ${table_comment}\n"
                    + "${partition_info}\n"
                    + "DISTRIBUTED BY HASH (${distribution_columns}) BUCKETS ${distribution_bucket}\n"
                    + "PROPERTIES (\n"
                    + "${properties}\n"
                    + ")\n";

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
    Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");
    Option<Long> DORIS_EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");
    Option<Boolean> SOURCE_USE_OLD_API =
            Options.key("source.use-old-api")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read data using the new interface defined according to the FLIP-27 specification,default false");

    // sink config options
    Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
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

    // create table

    SingleChoiceOption<DataSaveMode> SAVE_MODE =
            Options.key(SupportDataSaveMode.SAVE_MODE_KEY)
                    .singleChoice(
                            DataSaveMode.class,
                            Collections.singletonList(DataSaveMode.KEEP_SCHEMA_AND_DATA))
                    .noDefaultValue()
                    .withDescription(
                            "Table structure and data processing methods that already exist on the target end");

    Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (\n"
                                    + "${rowtype_fields}\n"
                                    + ") ENGINE=OLAP\n"
                                    + "UNIQUE KEY (${rowtype_primary_key})\n"
                                    + "DISTRIBUTED BY HASH (${rowtype_primary_key})\n"
                                    + "PROPERTIES (\n"
                                    + "    \"replication_num\" = \"1\" \n"
                                    + ")")
                    .withDescription("Create table statement template, used to create Doris table");

    OptionRule.Builder SINK_RULE =
            OptionRule.builder().required(FENODES, USERNAME, PASSWORD, TABLE_IDENTIFIER);

    OptionRule.Builder CATALOG_RULE =
            OptionRule.builder()
                    .required(FENODES, QUERY_PORT, USERNAME, PASSWORD)
                    .optional(SAVE_MODE)
                    .conditional(SAVE_MODE, DataSaveMode.KEEP_SCHEMA_AND_DATA);
}

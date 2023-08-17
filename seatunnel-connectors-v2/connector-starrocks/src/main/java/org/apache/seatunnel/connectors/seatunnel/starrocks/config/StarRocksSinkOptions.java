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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.StreamLoadFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuppressWarnings("MagicNumber")
public interface StarRocksSinkOptions {
    Option<List<String>> NODE_URLS =
            Options.key("nodeUrls")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "StarRocks cluster http address, the format is [\"fe_ip:fe_http_port\", ...]");

    Option<String> LABEL_PREFIX =
            Options.key("labelPrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prefix of StarRocks stream load label");

    Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks database");

    Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of StarRocks table");

    Option<String> SAVE_MODE_CREATE_TEMPLATE =
            Options.key("save_mode_create_template")
                    .stringType()
                    .defaultValue(
                            "CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (\n"
                                    + "${rowtype_fields}\n"
                                    + ") ENGINE=OLAP\n"
                                    + " PRIMARY KEY (${rowtype_primary_key})\n"
                                    + "DISTRIBUTED BY HASH (${rowtype_primary_key})"
                                    + "PROPERTIES (\n"
                                    + "    \"replication_num\" = \"1\" \n"
                                    + ")")
                    .withDescription(
                            "Create table statement template, used to create StarRocks table");

    Option<Integer> BATCH_MAX_SIZE =
            Options.key("batch_max_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    Option<Long> BATCH_MAX_BYTES =
            Options.key("batch_max_bytes")
                    .longType()
                    .defaultValue((long) (5 * 1024 * 1024))
                    .withDescription(
                            "For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    Option<Integer> BATCH_INTERVAL_MS =
            Options.key("batch_interval_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches batch_interval_ms, the data will be flushed into the StarRocks");

    Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of retries to flush failed");

    Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Using as a multiplier for generating the next delay for backoff");

    Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The amount of time to wait before attempting to retry a request to StarRocks");

    Option<Boolean> ENABLE_UPSERT_DELETE =
            Options.key("enable_upsert_delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable upsert/delete, only supports PrimaryKey model.");

    Option<Map<String, String>> STARROCKS_CONFIG =
            Options.key("starrocks.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the stream load data_desc. "
                                    + "The way to specify the parameter is to add the original stream load parameter into map");

    Option<String> COLUMN_SEPARATOR =
            Options.key("starrocks.config.column_separator")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    Option<StreamLoadFormat> LOAD_FORMAT =
            Options.key("starrocks.config.format")
                    .enumType(StreamLoadFormat.class)
                    .defaultValue(StreamLoadFormat.JSON)
                    .withDescription("");

    SingleChoiceOption<DataSaveMode> SAVE_MODE =
            Options.key(SupportDataSaveMode.SAVE_MODE_KEY)
                    .singleChoice(
                            DataSaveMode.class, Arrays.asList(DataSaveMode.KEEP_SCHEMA_AND_DATA))
                    .defaultValue(DataSaveMode.KEEP_SCHEMA_AND_DATA)
                    .withDescription(
                            "Table structure and data processing methods that already exist on the target end");
}

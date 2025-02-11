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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig.StreamLoadFormat;

import java.util.Map;

@SuppressWarnings("MagicNumber")
public class StarRocksSinkOptions extends StarRocksBaseOptions {

    public static final Option<String> BASE_URL =
            Options.key("base-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The JDBC URL like \"jdbc:mysql://localhost:9030/\" or"
                                    + "\"jdbc:mysql://localhost:9030/\" or \"jdbc:mysql://localhost:9030/db\"");
    public static final Option<String> LABEL_PREFIX =
            Options.key("labelPrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prefix of StarRocks stream load label");

    public static final Option<String> SAVE_MODE_CREATE_TEMPLATE =
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
                                    + " PRIMARY KEY ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")\n"
                                    + "COMMENT '"
                                    + SaveModePlaceHolder.COMMENT.getPlaceHolder()
                                    + "'\n"
                                    + "DISTRIBUTED BY HASH ("
                                    + SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder()
                                    + ")"
                                    + "PROPERTIES (\n"
                                    + "    \"replication_num\" = \"1\" \n"
                                    + ")")
                    .withDescription(
                            "Create table statement template, used to create StarRocks table");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of retries to flush failed");
    public static final Option<Integer> BATCH_MAX_SIZE =
            Options.key("batch_max_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches checkpoint.interval, the data will be flushed into the StarRocks");

    public static final Option<Long> BATCH_MAX_BYTES =
            Options.key("batch_max_bytes")
                    .longType()
                    .defaultValue((long) (5 * 1024 * 1024))
                    .withDescription(
                            "For batch writing, when the number of buffers reaches the number of batch_max_rows or the byte size of batch_max_bytes or the time reaches checkpoint.interval, the data will be flushed into the StarRocks");

    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Using as a multiplier for generating the next delay for backoff");

    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The amount of time to wait before attempting to retry a request to StarRocks");

    public static final Option<Boolean> ENABLE_UPSERT_DELETE =
            Options.key("enable_upsert_delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable upsert/delete, only supports PrimaryKey model.");

    public static final Option<Map<String, String>> STARROCKS_CONFIG =
            Options.key("starrocks.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the stream load data_desc. "
                                    + "The way to specify the parameter is to add the original stream load parameter into map");

    public static final Option<String> COLUMN_SEPARATOR =
            Options.key("starrocks.config.column_separator")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final Option<StreamLoadFormat> LOAD_FORMAT =
            Options.key("starrocks.config.format")
                    .enumType(StreamLoadFormat.class)
                    .defaultValue(StreamLoadFormat.JSON)
                    .withDescription("");
    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription(
                            "different treatment schemes are selected for the existing surface structure of the target side");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription(
                            "different processing schemes are selected for data existing data on the target side");

    public static final Option<Integer> HTTP_SOCKET_TIMEOUT_MS =
            Options.key("http_socket_timeout_ms")
                    .intType()
                    .defaultValue(3 * 60 * 1000)
                    .withDescription("Set http socket timeout, default is 3 minutes.");

    public static final Option<String> CUSTOM_SQL =
            Options.key("custom_sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("when schema_save_mode selects CUSTOM_PROCESSING custom SQL");
}

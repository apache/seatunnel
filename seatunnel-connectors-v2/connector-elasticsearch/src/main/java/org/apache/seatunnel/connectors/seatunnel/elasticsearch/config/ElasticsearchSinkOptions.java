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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;

public class ElasticsearchSinkOptions extends ElasticsearchBaseOptions {

    public static final Option<String> INDEX_TYPE =
            Options.key("index_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch index type, it is recommended not to specify in elasticsearch 6 and above");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Primary key fields used to generate the document `_id`");

    public static final Option<String> KEY_DELIMITER =
            Options.key("key_delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys (\"_\" by default), e.g., \"$\" would result in document `_id` \"KEY1$KEY2$KEY3\".");

    public static final Option<Integer> MAX_BATCH_SIZE =
            Options.key("max_batch_size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("batch bulk doc max size");

    public static final Option<Integer> MAX_RETRY_COUNT =
            Options.key("max_retry_count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("one bulk request max try count");

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
}

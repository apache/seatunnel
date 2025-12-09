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

package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;

import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;

public class MongodbSinkOptions extends MongodbBaseOptions {

    public static final Option<Integer> BUFFER_FLUSH_MAX_ROWS =
            Options.key("buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Specifies the maximum number of buffered rows per batch request.");

    public static final Option<Long> BUFFER_FLUSH_INTERVAL =
            Options.key("buffer-flush.interval")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Specifies the maximum interval of buffered rows per batch request, the unit is millisecond.");

    public static final Option<Integer> RETRY_MAX =
            Options.key("retry.max")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max number of retry if writing records to database failed.");

    public static final Option<Long> RETRY_INTERVAL =
            Options.key("retry.interval")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Specifies the retry time interval if writing records to database failed.");

    public static final Option<Boolean> UPSERT_ENABLE =
            Options.key("upsert-enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to write documents via upsert mode.");

    public static final Option<List<String>> PRIMARY_KEY =
            Options.key("primary-key")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The primary keys for upsert/update. Keys are in csv format for properties.")
                    .withFallbackKeys("upsert-key");

    public static final Option<Boolean> TRANSACTION =
            Options.key("transaction").booleanType().defaultValue(false).withDescription(".");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .singleChoice(
                            DataSaveMode.class,
                            Arrays.asList(DROP_DATA, APPEND_DATA, ERROR_WHEN_DATA_EXISTS))
                    .defaultValue(APPEND_DATA)
                    .withDescription("The save mode of collection data");
}

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

package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Arrays;
import java.util.Map;

import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;

public class BigtableSinkOptions extends BigtableBaseOptions {

    public static final Option<Map<String, String>> COLUMN_FAMILY =
            Options.key("column_family")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Mapping from column name to column family. "
                                    + "Use \"all_columns\" as key to set a default family for unmapped columns.");

    public static final Option<String> ROWKEY_DELIMITER =
            Options.key("rowkey_delimiter")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Delimiter used to join multiple rowkey column values. Default is empty string.");

    public static final Option<String> VERSION_COLUMN =
            Options.key("version_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Column name whose long value is used as the Bigtable cell timestamp. "
                                    + "If not set, the current system time is used.");

    public static final Option<NullMode> NULL_MODE =
            Options.key("null_mode")
                    .enumType(NullMode.class)
                    .defaultValue(NullMode.SKIP)
                    .withDescription(
                            "How to handle null field values: SKIP (default) omits the cell; EMPTY writes an empty byte array.");

    public static final Option<Long> TTL =
            Options.key("ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Cell TTL in milliseconds. -1 (default) means no expiration. "
                                    + "Note: Bigtable TTL is configured per column family, not per mutation.");

    public static final Option<Integer> BATCH_MUTATION_SIZE =
            Options.key("batch_mutation_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Number of mutations to accumulate before flushing to Bigtable. Default is 100.");

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

    public enum NullMode {
        SKIP,
        EMPTY;
    }
}

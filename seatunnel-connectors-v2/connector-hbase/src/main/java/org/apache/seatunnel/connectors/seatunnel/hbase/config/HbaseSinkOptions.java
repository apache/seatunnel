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

package org.apache.seatunnel.connectors.seatunnel.hbase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import java.util.Arrays;
import java.util.Map;

import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;

public class HbaseSinkOptions extends HbaseBaseOptions {

    private static final Integer DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final Option<Map<String, String>> FAMILY_NAME =
            Options.key("family_name")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Hbase column family name");

    public static final Option<String> ROWKEY_DELIMITER =
            Options.key("rowkey_delimiter")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Hbase rowkey join delimiter");

    public static final Option<String> VERSION_COLUMN =
            Options.key("version_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Hbase record version column used for assigning timestamp of records");

    public static final Option<NullMode> NULL_MODE =
            Options.key("null_mode")
                    .enumType(NullMode.class)
                    .defaultValue(NullMode.SKIP)
                    .withDescription("The processing mode for writing null values");

    public static final Option<Boolean> WAL_WRITE =
            Options.key("wal_write")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("The flag of whether write wal log");

    public static final Option<Integer> WRITE_BUFFER_SIZE =
            Options.key("write_buffer_size")
                    .intType()
                    .defaultValue(DEFAULT_BUFFER_SIZE)
                    .withDescription("Hbase client write buffer size");

    public static final Option<EnCoding> ENCODING =
            Options.key("encoding")
                    .enumType(EnCoding.class)
                    .defaultValue(EnCoding.UTF8)
                    .withDescription("Hbase record encoding");

    public static final Option<Long> HBASE_TTL_CONFIG =
            Options.key("ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The expiration time configuration for writing hbase data. The default value is -1, indicating no expiration time.");

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

    public enum EnCoding {
        UTF8,
        GBK;
    }
}

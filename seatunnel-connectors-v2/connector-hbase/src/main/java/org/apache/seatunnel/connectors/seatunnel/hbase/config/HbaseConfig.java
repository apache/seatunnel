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

import java.util.List;
import java.util.Map;

public class HbaseConfig {

    private static final Integer DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final Option<String> ZOOKEEPER_QUORUM =
            Options.key("zookeeper_quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hbase zookeeper quorum");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("Hbase table name");

    public static final Option<List<String>> ROWKEY_COLUMNS =
            Options.key("rowkey_column")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Hbase rowkey column");

    public static final Option<List<String>> QUERY_COLUMNS =
            Options.key("query_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("query Hbase columns");

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

    public static final Option<Map<String, String>> FAMILY_NAME =
            Options.key("family_name")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Hbase column family name");

    public static final Option<Map<String, String>> HBASE_EXTRA_CONFIG =
            Options.key("hbase_extra_config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Hbase extra config");

    public enum NullMode {
        SKIP,
        EMPTY;
    }

    public enum EnCoding {
        UTF8,
        GBK;
    }

    private HbaseConfig() {}
}

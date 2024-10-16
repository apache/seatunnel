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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Slf4j
public class PaimonSinkConfig extends PaimonConfig {
    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .enumType(DataSaveMode.class)
                    .defaultValue(DataSaveMode.APPEND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<String> PRIMARY_KEYS =
            Options.key("paimon.table.primary-keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default comma-separated list of columns that identify a row in tables (primary key)");

    public static final Option<String> PARTITION_KEYS =
            Options.key("paimon.table.partition-keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default comma-separated list of partition fields to use when creating tables.");

    public static final Option<Map<String, String>> WRITE_PROPS =
            Options.key("paimon.table.write-props")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Properties passed through to paimon table initialization, such as 'file.format', 'bucket'(org.apache.paimon.CoreOptions)");

    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private List<String> primaryKeys;
    private List<String> partitionKeys;
    private Map<String, String> writeProps;

    public PaimonSinkConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
        this.schemaSaveMode = readonlyConfig.get(SCHEMA_SAVE_MODE);
        this.dataSaveMode = readonlyConfig.get(DATA_SAVE_MODE);
        this.primaryKeys = stringToList(readonlyConfig.get(PRIMARY_KEYS), ",");
        this.partitionKeys = stringToList(readonlyConfig.get(PARTITION_KEYS), ",");
        this.writeProps = readonlyConfig.get(WRITE_PROPS);
        checkConfig();
    }

    private void checkConfig() {
        if (this.primaryKeys.isEmpty() && "-1".equals(this.writeProps.get("bucket"))) {
            log.warn("Append only table currently do not support dynamic bucket");
        }
    }
}

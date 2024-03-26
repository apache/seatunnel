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

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Getter
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

    private String catalogName;
    private String warehouse;
    private String namespace;
    private String table;
    private String hdfsSitePath;
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;

    public PaimonSinkConfig(ReadonlyConfig readonlyConfig) {
        this.catalogName = checkArgumentNotNull(readonlyConfig.get(CATALOG_NAME));
        this.warehouse = checkArgumentNotNull(readonlyConfig.get(WAREHOUSE));
        this.namespace = checkArgumentNotNull(readonlyConfig.get(DATABASE));
        this.table = checkArgumentNotNull(readonlyConfig.get(TABLE));
        this.hdfsSitePath = readonlyConfig.get(HDFS_SITE_PATH);
        this.schemaSaveMode = readonlyConfig.get(SCHEMA_SAVE_MODE);
        this.dataSaveMode = readonlyConfig.get(DATA_SAVE_MODE);
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }
}

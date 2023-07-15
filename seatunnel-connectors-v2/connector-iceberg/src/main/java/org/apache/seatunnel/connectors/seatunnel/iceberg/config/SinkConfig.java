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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.SupportedFileFormat;

import org.apache.iceberg.FileFormat;

import lombok.Getter;

import java.util.List;

public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = 1L;

    public static final Option<Boolean> ENABLE_UPSERT =
            Options.key("enable_upsert")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable upsert");

    public static final Option<List<String>> PRIMARY_KEYS =
            Options.key("primary_keys").listType().noDefaultValue().withDescription("primary keys");

    public static final Option<SupportedFileFormat> FILE_FORMAT =
            Options.key("file_format")
                    .enumType(SupportedFileFormat.class)
                    .defaultValue(SupportedFileFormat.PARQUET)
                    .withDescription("file format");

    @Getter private boolean enableUpsert = true;
    @Getter private List<String> primaryKeys;
    @Getter private FileFormat fileFormat = FileFormat.PARQUET;

    public SinkConfig(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        if (pluginConfig.getOptional(ENABLE_UPSERT).isPresent()) {
            this.enableUpsert = pluginConfig.getOptional(ENABLE_UPSERT).get();
        }
        if (pluginConfig.getOptional(PRIMARY_KEYS).isPresent()) {
            this.primaryKeys = pluginConfig.getOptional(PRIMARY_KEYS).get();
        }
        if (pluginConfig.getOptional(FILE_FORMAT).isPresent()) {
            this.fileFormat =
                    FileFormat.valueOf(pluginConfig.getOptional(FILE_FORMAT).get().name());
        }
    }
}

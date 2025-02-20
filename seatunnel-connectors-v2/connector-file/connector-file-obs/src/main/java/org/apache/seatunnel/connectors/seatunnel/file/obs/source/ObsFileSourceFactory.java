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

package org.apache.seatunnel.connectors.seatunnel.file.obs.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.obs.config.ObsConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class ObsFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.OBS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ObsConfig.FILE_PATH)
                .required(ObsConfig.BUCKET)
                .required(ObsConfig.ACCESS_KEY)
                .required(ObsConfig.ACCESS_SECRET)
                .required(ObsConfig.ENDPOINT)
                .required(BaseSourceConfigOptions.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfigOptions.FIELD_DELIMITER)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.EXCEL, FileFormat.CSV),
                        ConnectorCommonOptions.SCHEMA)
                .optional(BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfigOptions.DATE_FORMAT)
                .optional(BaseSourceConfigOptions.DATETIME_FORMAT)
                .optional(BaseSourceConfigOptions.TIME_FORMAT)
                .optional(BaseSourceConfigOptions.NULL_FORMAT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ObsFileSource.class;
    }
}

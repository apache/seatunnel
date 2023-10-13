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

package org.apache.seatunnel.connectors.seatunnel.file.oss.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class OssFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(OssConfig.FILE_PATH)
                .required(OssConfig.BUCKET)
                .required(OssConfig.ACCESS_KEY)
                .required(OssConfig.ACCESS_SECRET)
                .required(OssConfig.ENDPOINT)
                .required(BaseSourceConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfig.DELIMITER)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.EXCEL, FileFormat.CSV),
                        TableSchemaOptions.SCHEMA)
                .optional(BaseSourceConfig.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfig.DATE_FORMAT)
                .optional(BaseSourceConfig.DATETIME_FORMAT)
                .optional(BaseSourceConfig.TIME_FORMAT)
                .optional(BaseSourceConfig.FILE_FILTER_PATTERN)
                .optional(BaseSourceConfig.COMPRESS_CODEC)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OssFileSource.class;
    }
}

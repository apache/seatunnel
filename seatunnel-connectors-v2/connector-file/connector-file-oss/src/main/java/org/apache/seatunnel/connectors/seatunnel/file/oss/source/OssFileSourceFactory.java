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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.oss.config.OssFileSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Arrays;

@AutoService(Factory.class)
public class OssFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new OssFileSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(ConnectorCommonOptions.TABLE_CONFIGS)
                .required(FileBaseOptions.FILE_PATH)
                .required(OssFileSourceOptions.BUCKET)
                .required(OssFileSourceOptions.ACCESS_KEY)
                .required(OssFileSourceOptions.ACCESS_SECRET)
                .required(OssFileSourceOptions.ENDPOINT)
                .required(FileBaseSourceOptions.FILE_FORMAT_TYPE)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        FileBaseSourceOptions.XML_ROW_TAG,
                        FileBaseSourceOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        ConnectorCommonOptions.SCHEMA)
                .optional(FileBaseSourceOptions.ENCODING)
                .optional(FileBaseSourceOptions.READ_COLUMNS)
                .optional(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH)
                .optional(FileBaseSourceOptions.FIELD_DELIMITER)
                .optional(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .optional(FileBaseSourceOptions.DATE_FORMAT)
                .optional(FileBaseSourceOptions.DATETIME_FORMAT)
                .optional(FileBaseSourceOptions.TIME_FORMAT)
                .optional(FileBaseSourceOptions.SHEET_NAME)
                .optional(FileBaseSourceOptions.FILE_FILTER_PATTERN)
                .optional(FileBaseSourceOptions.NULL_FORMAT)
                .optional(FileBaseSourceOptions.FILENAME_EXTENSION)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OssFileSource.class;
    }
}

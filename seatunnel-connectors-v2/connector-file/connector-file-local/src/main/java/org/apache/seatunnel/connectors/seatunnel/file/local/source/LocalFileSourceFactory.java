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

package org.apache.seatunnel.connectors.seatunnel.file.local.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Arrays;

@AutoService(Factory.class)
public class LocalFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new LocalFileSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .exclusive(LocalFileSourceOptions.TABLE_CONFIGS, FileBaseOptions.FILE_PATH)
                .optional(LocalFileSourceOptions.FILE_FORMAT_TYPE)
                .conditional(
                        LocalFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        FileBaseSourceOptions.FIELD_DELIMITER,
                        FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        LocalFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        LocalFileSourceOptions.XML_ROW_TAG,
                        FileBaseSinkOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        LocalFileSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        ConnectorCommonOptions.SCHEMA)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        FileBaseSourceOptions.ENCODING)
                .optional(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH)
                .optional(FileBaseSourceOptions.DATE_FORMAT)
                .optional(FileBaseSourceOptions.DATETIME_FORMAT)
                .optional(FileBaseSourceOptions.TIME_FORMAT)
                .optional(FileBaseSourceOptions.FILE_FILTER_PATTERN)
                .optional(FileBaseSourceOptions.COMPRESS_CODEC)
                .optional(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC)
                .optional(FileBaseSourceOptions.NULL_FORMAT)
                .optional(FileBaseSourceOptions.FILENAME_EXTENSION)
                .optional(FileBaseSourceOptions.READ_COLUMNS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return LocalFileSource.class;
    }
}

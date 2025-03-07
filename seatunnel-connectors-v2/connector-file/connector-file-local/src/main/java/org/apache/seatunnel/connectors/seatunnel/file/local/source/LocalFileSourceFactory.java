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
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.DATETIME_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.DATE_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.ENCODING;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.FILE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.SHEET_NAME;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.TIME_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions.XML_USE_ATTR_FORMAT;

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
                .exclusive(LocalFileSourceOptions.TABLE_CONFIGS, FILE_PATH)
                .optional(LocalFileSourceOptions.FILE_FORMAT_TYPE)
                .conditional(
                        LocalFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        LocalFileSourceOptions.XML_ROW_TAG,
                        XML_USE_ATTR_FORMAT)
                .conditional(
                        LocalFileSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        ConnectorCommonOptions.SCHEMA)
                .optional(ENCODING)
                .optional(LocalFileSourceOptions.READ_COLUMNS)
                .optional(LocalFileSourceOptions.PARSE_PARTITION_FROM_PATH)
                .optional(LocalFileSourceOptions.FIELD_DELIMITER)
                .optional(LocalFileSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .optional(DATE_FORMAT)
                .optional(DATETIME_FORMAT)
                .optional(TIME_FORMAT)
                .optional(SHEET_NAME)
                .optional(LocalFileSourceOptions.EXCEL_ENGINE)
                .optional(LocalFileSourceOptions.FILE_FILTER_PATTERN)
                .optional(LocalFileSourceOptions.NULL_FORMAT)
                .optional(LocalFileSourceOptions.FILENAME_EXTENSION)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return LocalFileSource.class;
    }
}

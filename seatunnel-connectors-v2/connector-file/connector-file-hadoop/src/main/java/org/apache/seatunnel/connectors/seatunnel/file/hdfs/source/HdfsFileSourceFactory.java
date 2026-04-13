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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSyncMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Arrays;

@AutoService(Factory.class)
public class HdfsFileSourceFactory implements TableSourceFactory {

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new HdfsFileSource(context.getOptions(), discoverTableSchemas(context));
    }

    @Override
    public String factoryIdentifier() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .exclusive(HdfsFileSourceOptions.TABLE_CONFIGS, HdfsFileSourceOptions.FILE_PATH)
                .optional(HdfsFileSourceOptions.DEFAULT_FS)
                .optional(HdfsFileSourceOptions.FILE_FORMAT_TYPE)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        HdfsFileSourceOptions.ROW_DELIMITER,
                        HdfsFileSourceOptions.FIELD_DELIMITER,
                        HdfsFileSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        HdfsFileSourceOptions.XML_ROW_TAG,
                        HdfsFileSourceOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        HdfsFileSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        ConnectorCommonOptions.SCHEMA)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        HdfsFileSourceOptions.ENCODING)
                .conditional(
                        HdfsFileSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.CSV,
                                FileFormat.PARQUET),
                        HdfsFileSourceOptions.ENABLE_FILE_SPLIT)
                .conditional(
                        HdfsFileSourceOptions.ENABLE_FILE_SPLIT,
                        Boolean.TRUE,
                        HdfsFileSourceOptions.FILE_SPLIT_SIZE)
                .optional(HdfsFileSourceOptions.PARSE_PARTITION_FROM_PATH)
                .optional(HdfsFileSourceOptions.DATE_FORMAT_LEGACY)
                .optional(HdfsFileSourceOptions.DATETIME_FORMAT_LEGACY)
                .optional(HdfsFileSourceOptions.TIME_FORMAT_LEGACY)
                .optional(HdfsFileSourceOptions.FILE_FILTER_PATTERN)
                .optional(HdfsFileSourceOptions.COMPRESS_CODEC)
                .optional(HdfsFileSourceOptions.ARCHIVE_COMPRESS_CODEC)
                .optional(HdfsFileSourceOptions.NULL_FORMAT)
                .optional(HdfsFileSourceOptions.FILENAME_EXTENSION)
                .optional(HdfsFileSourceOptions.READ_COLUMNS)
                .optional(
                        HdfsFileSourceOptions.DISCOVERY_MODE,
                        HdfsFileSourceOptions.SCAN_INTERVAL,
                        HdfsFileSourceOptions.START_MODE)
                .optional(
                        HdfsFileSourceOptions.SYNC_MODE,
                        HdfsFileSourceOptions.TARGET_HADOOP_CONF,
                        HdfsFileSourceOptions.UPDATE_STRATEGY,
                        HdfsFileSourceOptions.COMPARE_MODE)
                .conditional(
                        HdfsFileSourceOptions.SYNC_MODE,
                        FileSyncMode.UPDATE,
                        HdfsFileSourceOptions.TARGET_PATH)
                .optional(HdfsFileSourceOptions.HDFS_SITE_PATH)
                .optional(HdfsFileSourceOptions.KERBEROS_PRINCIPAL)
                .optional(HdfsFileSourceOptions.KERBEROS_KEYTAB_PATH)
                .optional(HdfsFileSourceOptions.KRB5_PATH)
                .optional(HdfsFileSourceOptions.REMOTE_USER)
                .optional(HdfsFileSourceOptions.QUOTE_CHAR)
                .optional(HdfsFileSourceOptions.ESCAPE_CHAR)
                .optional(ConnectorCommonOptions.METALAKE_TYPE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HdfsFileSource.class;
    }
}

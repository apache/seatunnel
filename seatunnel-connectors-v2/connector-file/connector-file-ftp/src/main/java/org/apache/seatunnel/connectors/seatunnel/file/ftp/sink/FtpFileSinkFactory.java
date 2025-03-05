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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.factory.BaseMultipleTableFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpFileSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class FtpFileSinkFactory extends BaseMultipleTableFileSinkFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(FtpFileSinkOptions.FILE_PATH)
                .required(FtpFileSinkOptions.FTP_HOST)
                .required(FtpFileSinkOptions.FTP_PORT)
                .required(FtpFileSinkOptions.FTP_USERNAME)
                .required(FtpFileSinkOptions.FTP_PASSWORD)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .optional(FileBaseSinkOptions.TMP_PATH)
                .optional(FileBaseSinkOptions.FILE_FORMAT_TYPE)
                .optional(FileBaseSinkOptions.SCHEMA_SAVE_MODE)
                .optional(FileBaseSinkOptions.DATA_SAVE_MODE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        FileBaseSinkOptions.XML_USE_ATTR_FORMAT)
                .optional(FileBaseSinkOptions.CUSTOM_FILENAME)
                .conditional(
                        FileBaseSinkOptions.CUSTOM_FILENAME,
                        true,
                        FileBaseSinkOptions.FILE_NAME_EXPRESSION)
                .optional(FileBaseSinkOptions.HAVE_PARTITION)
                .conditional(
                        FileBaseSinkOptions.HAVE_PARTITION,
                        true,
                        FileBaseSinkOptions.PARTITION_BY,
                        FileBaseSinkOptions.PARTITION_DIR_EXPRESSION,
                        FileBaseSinkOptions.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(FileBaseSinkOptions.SINK_COLUMNS)
                .optional(FileBaseSinkOptions.COMPRESS_CODEC)
                .optional(FileBaseSinkOptions.ENABLE_HEADER_WRITE)
                .optional(FileBaseSinkOptions.FIELD_DELIMITER)
                .optional(FileBaseSinkOptions.ROW_DELIMITER)
                .optional(FileBaseSinkOptions.IS_ENABLE_TRANSACTION)
                .optional(FileBaseSinkOptions.FILENAME_TIME_FORMAT)
                .optional(FileBaseSinkOptions.MAX_ROWS_IN_MEMORY)
                .optional(FileBaseSinkOptions.SHEET_NAME)
                .optional(FileBaseSinkOptions.DATE_FORMAT)
                .optional(FileBaseSinkOptions.DATETIME_FORMAT)
                .optional(FileBaseSinkOptions.TIME_FORMAT)
                .optional(FileBaseSinkOptions.XML_ROOT_TAG)
                .optional(FileBaseSinkOptions.XML_ROW_TAG)
                .optional(FileBaseSinkOptions.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .optional(FileBaseSinkOptions.PARQUET_AVRO_WRITE_FIXED_AS_INT96)
                .optional(FtpFileSinkOptions.FTP_CONNECTION_MODE)
                .optional(FileBaseSinkOptions.SINGLE_FILE_MODE)
                .optional(FileBaseSinkOptions.ENCODING)
                .optional(FileBaseSinkOptions.BATCH_SIZE)
                .optional(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA)
                .optional(FileBaseSinkOptions.FILENAME_EXTENSION)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new FtpFileSink(readonlyConfig, catalogTable);
    }
}

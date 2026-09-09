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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.factory.BaseMultipleTableFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.config.GcsFileSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import org.apache.hadoop.fs.Path;

import com.google.auto.service.AutoService;

import java.util.Arrays;

/** Creates Google Cloud Storage file sinks from table factory configuration. */
@AutoService(Factory.class)
public class GcsFileSinkFactory extends BaseMultipleTableFileSinkFactory {

    @Override
    public String factoryIdentifier() {
        return FileSystemType.GCS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(FileBaseSinkOptions.FILE_PATH)
                .required(GcsFileSinkOptions.BUCKET)
                .optional(
                        GcsFileSinkOptions.SERVICE_ACCOUNT_KEY_FILE,
                        GcsFileSinkOptions.GCS_PROPERTIES)
                .optional(FileBaseSinkOptions.SCHEMA_SAVE_MODE)
                .optional(FileBaseSinkOptions.DATA_SAVE_MODE)
                .optional(FileBaseSinkOptions.FILE_FORMAT_TYPE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.FIELD_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS,
                        FileBaseSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS,
                        FileBaseSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        FileBaseSinkOptions.ORC_COMPRESS)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        FileBaseSinkOptions.PARQUET_COMPRESS,
                        FileBaseSinkOptions.PARQUET_AVRO_WRITE_FIXED_AS_INT96,
                        FileBaseSinkOptions.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        FileBaseSinkOptions.XML_USE_ATTR_FORMAT,
                        FileBaseSinkOptions.XML_ROOT_TAG,
                        FileBaseSinkOptions.XML_ROW_TAG)
                .optional(FileBaseSinkOptions.CUSTOM_FILENAME)
                .conditional(
                        FileBaseSinkOptions.CUSTOM_FILENAME,
                        true,
                        FileBaseSinkOptions.FILE_NAME_EXPRESSION,
                        FileBaseSinkOptions.FILENAME_TIME_FORMAT)
                .optional(FileBaseSinkOptions.HAVE_PARTITION)
                .conditional(
                        FileBaseSinkOptions.HAVE_PARTITION,
                        true,
                        FileBaseSinkOptions.PARTITION_BY,
                        FileBaseSinkOptions.PARTITION_DIR_EXPRESSION,
                        FileBaseSinkOptions.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        FileBaseSinkOptions.ENCODING)
                .optional(FileBaseSinkOptions.SINK_COLUMNS)
                .optional(FileBaseSinkOptions.IS_ENABLE_TRANSACTION)
                .optional(FileBaseSinkOptions.DATE_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.DATETIME_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.TIME_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.SINGLE_FILE_MODE)
                .optional(FileBaseSinkOptions.BATCH_SIZE)
                .optional(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .optional(FileBaseSinkOptions.FILENAME_EXTENSION)
                .optional(FileBaseSinkOptions.TMP_PATH)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        validateDestructivePath(readonlyConfig);
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new GcsFileSink(readonlyConfig, catalogTable);
    }

    private static void validateDestructivePath(ReadonlyConfig readonlyConfig) {
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(FileBaseSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(FileBaseSinkOptions.DATA_SAVE_MODE);
        if (schemaSaveMode != SchemaSaveMode.RECREATE_SCHEMA
                && dataSaveMode != DataSaveMode.DROP_DATA) {
            return;
        }

        String filePath = readonlyConfig.get(FileBaseSinkOptions.FILE_PATH);
        String normalizedPath = new Path(filePath).toUri().normalize().getPath();
        if (normalizedPath == null || normalizedPath.isEmpty() || "/".equals(normalizedPath)) {
            throw new IllegalArgumentException(
                    String.format(
                            "GcsFile path must point to a prefix below the bucket root when "
                                    + "schema_save_mode is %s or data_save_mode is %s",
                            schemaSaveMode, dataSaveMode));
        }
    }
}

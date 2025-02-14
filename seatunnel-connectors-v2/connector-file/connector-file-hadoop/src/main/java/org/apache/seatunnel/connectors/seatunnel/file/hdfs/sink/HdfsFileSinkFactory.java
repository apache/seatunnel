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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HdfsFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HdfsSourceConfigOptions.DEFAULT_FS)
                .required(BaseSinkConfig.FILE_PATH)
                .optional(BaseSinkConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSinkConfig.ROW_DELIMITER,
                        BaseSinkConfig.FIELD_DELIMITER,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        BaseSinkConfig.TXT_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        BaseSinkConfig.ORC_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        BaseSinkConfig.PARQUET_COMPRESS,
                        BaseSinkConfig.PARQUET_AVRO_WRITE_FIXED_AS_INT96,
                        BaseSinkConfig.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        BaseSinkConfig.XML_USE_ATTR_FORMAT)
                .optional(BaseSinkConfig.CUSTOM_FILENAME)
                .conditional(
                        BaseSinkConfig.CUSTOM_FILENAME,
                        true,
                        BaseSinkConfig.FILE_NAME_EXPRESSION,
                        BaseSinkConfig.FILENAME_TIME_FORMAT)
                .optional(BaseSinkConfig.HAVE_PARTITION)
                .conditional(
                        BaseSinkConfig.HAVE_PARTITION,
                        true,
                        BaseSinkConfig.PARTITION_BY,
                        BaseSinkConfig.PARTITION_DIR_EXPRESSION,
                        BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(BaseSinkConfig.SINK_COLUMNS)
                .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
                .optional(BaseSinkConfig.DATE_FORMAT)
                .optional(BaseSinkConfig.DATETIME_FORMAT)
                .optional(BaseSinkConfig.TIME_FORMAT)
                .optional(BaseSinkConfig.SINGLE_FILE_MODE)
                .optional(BaseSinkConfig.BATCH_SIZE)
                .optional(BaseSinkConfig.HDFS_SITE_PATH)
                .optional(BaseSinkConfig.KERBEROS_PRINCIPAL)
                .optional(BaseSinkConfig.KERBEROS_KEYTAB_PATH)
                .optional(BaseSinkConfig.KRB5_PATH)
                .optional(BaseSinkConfig.REMOTE_USER)
                .optional(BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA)
                .build();
    }
}

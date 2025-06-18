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

package org.apache.seatunnel.connectors.seatunnel.redshift.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfigOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class S3RedshiftFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "S3Redshift";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        S3FileBaseOptions.S3_BUCKET,
                        S3RedshiftConfigOptions.JDBC_URL,
                        S3RedshiftConfigOptions.JDBC_USER,
                        S3RedshiftConfigOptions.JDBC_PASSWORD,
                        S3RedshiftConfigOptions.EXECUTE_SQL,
                        FileBaseSourceOptions.FILE_PATH,
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER)
                .conditional(
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER,
                        S3FileBaseOptions.S3aAwsCredentialsProvider.SimpleAWSCredentialsProvider,
                        S3FileBaseOptions.S3_ACCESS_KEY,
                        S3FileBaseOptions.S3_SECRET_KEY)
                .optional(S3FileBaseOptions.S3_PROPERTIES)
                .optional(FileBaseSinkOptions.FILE_FORMAT_TYPE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        FileBaseSinkOptions.FIELD_DELIMITER,
                        FileBaseSinkOptions.ROW_DELIMITER)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        FileBaseSinkOptions.ROW_DELIMITER)
                .optional(FileBaseSinkOptions.PARTITION_BY)
                .optional(FileBaseSinkOptions.PARTITION_DIR_EXPRESSION)
                .optional(FileBaseSinkOptions.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(FileBaseSinkOptions.SINK_COLUMNS)
                .optional(FileBaseSinkOptions.IS_ENABLE_TRANSACTION)
                .optional(FileBaseSinkOptions.FILE_NAME_EXPRESSION)
                .build();
    }
}

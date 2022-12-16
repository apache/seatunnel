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
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;

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
            .required(S3Config.S3_BUCKET, S3RedshiftConfig.JDBC_URL, S3RedshiftConfig.JDBC_USER, S3RedshiftConfig.JDBC_PASSWORD, S3RedshiftConfig.EXECUTE_SQL, BaseSourceConfig.FILE_PATH)
            .optional(S3Config.S3_ACCESS_KEY, S3Config.S3_SECRET_KEY)
            .optional(BaseSinkConfig.FILE_FORMAT)
            .optional(BaseSinkConfig.FILENAME_TIME_FORMAT)
            .optional(BaseSinkConfig.FIELD_DELIMITER)
            .optional(BaseSinkConfig.ROW_DELIMITER)
            .optional(BaseSinkConfig.PARTITION_BY)
            .optional(BaseSinkConfig.PARTITION_DIR_EXPRESSION)
            .optional(BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
            .optional(BaseSinkConfig.SINK_COLUMNS)
            .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
            .optional(BaseSinkConfig.FILE_NAME_EXPRESSION)
            .build();
    }
}

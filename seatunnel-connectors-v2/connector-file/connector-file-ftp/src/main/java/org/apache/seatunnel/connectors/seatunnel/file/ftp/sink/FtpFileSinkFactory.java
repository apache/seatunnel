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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.config.FtpConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class FtpFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(FtpConfig.FTP_HOST)
                .required(FtpConfig.FTP_PORT)
                .required(FtpConfig.FTP_USERNAME)
                .required(FtpConfig.FTP_PASSWORD)
                .required(FtpConfig.FILE_PATH)
                .optional(BaseSinkConfig.FILE_NAME_EXPRESSION)
                .optional(BaseSinkConfig.FILE_FORMAT)
                .optional(BaseSinkConfig.FILENAME_TIME_FORMAT)
                .optional(BaseSinkConfig.FIELD_DELIMITER)
                .optional(BaseSinkConfig.ROW_DELIMITER)
                .optional(BaseSinkConfig.PARTITION_BY)
                .optional(BaseSinkConfig.PARTITION_DIR_EXPRESSION)
                .optional(BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(BaseSinkConfig.SINK_COLUMNS)
                .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
                .build();
    }
}

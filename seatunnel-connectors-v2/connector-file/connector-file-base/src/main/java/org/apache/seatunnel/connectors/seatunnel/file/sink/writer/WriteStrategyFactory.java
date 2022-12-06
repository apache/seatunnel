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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriteStrategyFactory {

    private WriteStrategyFactory() {}

    public static WriteStrategy of(String fileType, TextFileSinkConfig textFileSinkConfig) {
        try {
            FileFormat fileFormat = FileFormat.valueOf(fileType.toUpperCase());
            return fileFormat.getWriteStrategy(textFileSinkConfig);
        } catch (IllegalArgumentException e) {
            String errorMsg = String.format("File sink connector not support this file type [%s], please check your config", fileType);
            throw new FileConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    public static WriteStrategy of(FileFormat fileFormat, TextFileSinkConfig textFileSinkConfig) {
        return fileFormat.getWriteStrategy(textFileSinkConfig);
    }
}

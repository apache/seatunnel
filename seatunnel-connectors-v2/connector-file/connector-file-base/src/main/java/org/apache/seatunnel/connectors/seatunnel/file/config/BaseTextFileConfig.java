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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Locale;

@Data
public class BaseTextFileConfig implements DelimiterConfig, CompressConfig, Serializable {
    private static final long serialVersionUID = 1L;
    protected String compressCodec;
    protected String fieldDelimiter = BaseSinkConfig.FIELD_DELIMITER.defaultValue();
    protected String rowDelimiter = BaseSinkConfig.ROW_DELIMITER.defaultValue();
    protected String path;
    protected String fileNameExpression;
    protected FileFormat fileFormat = FileFormat.TEXT;
    protected DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    protected DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    protected TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    public BaseTextFileConfig(@NonNull Config config) {
        if (config.hasPath(BaseSinkConfig.COMPRESS_CODEC.key())) {
            throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Compress not supported by SeaTunnel file connector now");
        }

        if (config.hasPath(BaseSinkConfig.FIELD_DELIMITER.key()) &&
                StringUtils.isNotEmpty(config.getString(BaseSinkConfig.FIELD_DELIMITER.key()))) {
            this.fieldDelimiter = config.getString(BaseSinkConfig.FIELD_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.ROW_DELIMITER.key()) &&
                StringUtils.isNotEmpty(config.getString(BaseSinkConfig.ROW_DELIMITER.key()))) {
            this.rowDelimiter = config.getString(BaseSinkConfig.ROW_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_PATH.key()) && !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_PATH.key()))) {
            this.path = config.getString(BaseSinkConfig.FILE_PATH.key());
        }
        checkNotNull(path);

        if (config.hasPath(BaseSinkConfig.FILE_NAME_EXPRESSION.key()) &&
                !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key()))) {
            this.fileNameExpression = config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_FORMAT.key()) &&
                !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_FORMAT.key()))) {
            this.fileFormat = FileFormat.valueOf(config.getString(BaseSinkConfig.FILE_FORMAT.key()).toUpperCase(Locale.ROOT));
        }

        if (config.hasPath(BaseSinkConfig.DATE_FORMAT.key())) {
            dateFormat = DateUtils.Formatter.parse(config.getString(BaseSinkConfig.DATE_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.DATETIME_FORMAT.key())) {
            datetimeFormat = DateTimeUtils.Formatter.parse(config.getString(BaseSinkConfig.DATETIME_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.TIME_FORMAT.key())) {
            timeFormat = TimeUtils.Formatter.parse(config.getString(BaseSinkConfig.TIME_FORMAT.key()));
        }
    }

    public BaseTextFileConfig() {}
}

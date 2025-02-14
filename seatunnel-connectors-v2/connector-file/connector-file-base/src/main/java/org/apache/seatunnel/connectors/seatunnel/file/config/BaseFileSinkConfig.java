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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.NonNull;

import java.io.File;
import java.io.Serializable;
import java.util.Locale;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Data
public class BaseFileSinkConfig implements DelimiterConfig, Serializable {
    private static final long serialVersionUID = 1L;
    protected CompressFormat compressFormat = BaseSinkConfig.COMPRESS_CODEC.defaultValue();
    protected String fieldDelimiter = BaseSinkConfig.FIELD_DELIMITER.defaultValue();
    protected String rowDelimiter = BaseSinkConfig.ROW_DELIMITER.defaultValue();
    protected int batchSize = BaseSinkConfig.BATCH_SIZE.defaultValue();
    protected String path;
    protected String fileNameExpression = BaseSinkConfig.FILE_NAME_EXPRESSION.defaultValue();
    protected boolean singleFileMode = BaseSinkConfig.SINGLE_FILE_MODE.defaultValue();
    protected boolean createEmptyFileWhenNoData =
            BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA.defaultValue();
    protected FileFormat fileFormat = FileFormat.TEXT;
    protected DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    protected DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    protected TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;
    protected Boolean enableHeaderWriter = false;

    public BaseFileSinkConfig(@NonNull Config config) {
        if (config.hasPath(BaseSinkConfig.COMPRESS_CODEC.key())) {
            String compressCodec = config.getString(BaseSinkConfig.COMPRESS_CODEC.key());
            this.compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        if (config.hasPath(BaseSinkConfig.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(BaseSinkConfig.BATCH_SIZE.key());
        }
        if (config.hasPath(BaseSinkConfig.FIELD_DELIMITER.key())
                && StringUtils.isNotEmpty(config.getString(BaseSinkConfig.FIELD_DELIMITER.key()))) {
            this.fieldDelimiter = config.getString(BaseSinkConfig.FIELD_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.ROW_DELIMITER.key())) {
            this.rowDelimiter = config.getString(BaseSinkConfig.ROW_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_PATH.key())
                && !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_PATH.key()))) {
            this.path = config.getString(BaseSinkConfig.FILE_PATH.key());
        }
        checkNotNull(path);

        if (path.equals(File.separator)) {
            this.path = "";
        }

        if (config.hasPath(BaseSinkConfig.FILE_NAME_EXPRESSION.key())
                && !StringUtils.isBlank(
                        config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key()))) {
            this.fileNameExpression = config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key());
        }

        if (config.hasPath(BaseSinkConfig.SINGLE_FILE_MODE.key())) {
            this.singleFileMode = config.getBoolean(BaseSinkConfig.SINGLE_FILE_MODE.key());
        }

        if (config.hasPath(BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA.key())) {
            this.createEmptyFileWhenNoData =
                    config.getBoolean(BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_FORMAT_TYPE.key())
                && !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_FORMAT_TYPE.key()))) {
            this.fileFormat =
                    FileFormat.valueOf(
                            config.getString(BaseSinkConfig.FILE_FORMAT_TYPE.key())
                                    .toUpperCase(Locale.ROOT));
        }

        if (config.hasPath(BaseSinkConfig.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(config.getString(BaseSinkConfig.DATE_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            config.getString(BaseSinkConfig.DATETIME_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(config.getString(BaseSinkConfig.TIME_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.ENABLE_HEADER_WRITE.key())) {
            enableHeaderWriter = config.getBoolean(BaseSinkConfig.ENABLE_HEADER_WRITE.key());
        }
    }

    public BaseFileSinkConfig() {}
}

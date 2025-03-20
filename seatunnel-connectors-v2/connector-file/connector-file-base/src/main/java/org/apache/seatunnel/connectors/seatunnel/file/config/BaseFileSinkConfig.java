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
    protected CompressFormat compressFormat = FileBaseSinkOptions.COMPRESS_CODEC.defaultValue();
    protected String fieldDelimiter = FileBaseSinkOptions.FIELD_DELIMITER.defaultValue();
    protected String rowDelimiter = FileBaseSinkOptions.ROW_DELIMITER.defaultValue();
    protected int batchSize = FileBaseSinkOptions.BATCH_SIZE.defaultValue();
    protected String path;
    protected String fileNameExpression = FileBaseSinkOptions.FILE_NAME_EXPRESSION.defaultValue();
    protected boolean singleFileMode = FileBaseSinkOptions.SINGLE_FILE_MODE.defaultValue();
    protected boolean createEmptyFileWhenNoData =
            FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA.defaultValue();
    protected FileFormat fileFormat;
    protected String filenameExtension = FileBaseSinkOptions.FILENAME_EXTENSION.defaultValue();
    protected DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    protected DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    protected TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;
    protected Boolean enableHeaderWriter = false;

    public BaseFileSinkConfig(@NonNull Config config) {
        if (config.hasPath(FileBaseSinkOptions.COMPRESS_CODEC.key())) {
            String compressCodec = config.getString(FileBaseSinkOptions.COMPRESS_CODEC.key());
            this.compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        if (config.hasPath(FileBaseSinkOptions.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(FileBaseSinkOptions.BATCH_SIZE.key());
        }
        if (config.hasPath(FileBaseSinkOptions.FIELD_DELIMITER.key())
                && StringUtils.isNotEmpty(
                        config.getString(FileBaseSinkOptions.FIELD_DELIMITER.key()))) {
            this.fieldDelimiter = config.getString(FileBaseSinkOptions.FIELD_DELIMITER.key());
        }

        if (config.hasPath(FileBaseSinkOptions.ROW_DELIMITER.key())) {
            this.rowDelimiter = config.getString(FileBaseSinkOptions.ROW_DELIMITER.key());
        }

        if (config.hasPath(FileBaseSinkOptions.FILE_PATH.key())
                && !StringUtils.isBlank(config.getString(FileBaseSinkOptions.FILE_PATH.key()))) {
            this.path = config.getString(FileBaseSinkOptions.FILE_PATH.key());
        }
        checkNotNull(path);

        if (path.equals(File.separator)) {
            this.path = "";
        }

        if (config.hasPath(FileBaseSinkOptions.FILE_NAME_EXPRESSION.key())
                && !StringUtils.isBlank(
                        config.getString(FileBaseSinkOptions.FILE_NAME_EXPRESSION.key()))) {
            this.fileNameExpression =
                    config.getString(FileBaseSinkOptions.FILE_NAME_EXPRESSION.key());
        }

        if (config.hasPath(FileBaseSinkOptions.SINGLE_FILE_MODE.key())) {
            this.singleFileMode = config.getBoolean(FileBaseSinkOptions.SINGLE_FILE_MODE.key());
        }

        if (config.hasPath(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA.key())) {
            this.createEmptyFileWhenNoData =
                    config.getBoolean(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA.key());
        }

        if (config.hasPath(FileBaseSinkOptions.FILE_FORMAT_TYPE.key())
                && !StringUtils.isBlank(
                        config.getString(FileBaseSinkOptions.FILE_FORMAT_TYPE.key()))) {
            this.fileFormat =
                    FileFormat.valueOf(
                            config.getString(FileBaseSinkOptions.FILE_FORMAT_TYPE.key())
                                    .toUpperCase(Locale.ROOT));
        } else {
            // fall back to the default
            this.fileFormat = FileBaseSinkOptions.FILE_FORMAT_TYPE.defaultValue();
        }

        if (config.hasPath(FileBaseSinkOptions.FILENAME_EXTENSION.key())
                && !StringUtils.isBlank(
                        config.getString(FileBaseSinkOptions.FILENAME_EXTENSION.key()))) {
            this.filenameExtension = config.getString(FileBaseSinkOptions.FILENAME_EXTENSION.key());
        }

        if (config.hasPath(FileBaseSinkOptions.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            config.getString(FileBaseSinkOptions.DATE_FORMAT.key()));
        }

        if (config.hasPath(FileBaseSinkOptions.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            config.getString(FileBaseSinkOptions.DATETIME_FORMAT.key()));
        }

        if (config.hasPath(FileBaseSinkOptions.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            config.getString(FileBaseSinkOptions.TIME_FORMAT.key()));
        }

        if (config.hasPath(FileBaseSinkOptions.ENABLE_HEADER_WRITE.key())) {
            enableHeaderWriter = config.getBoolean(FileBaseSinkOptions.ENABLE_HEADER_WRITE.key());
        }
    }

    public BaseFileSinkConfig() {}
}

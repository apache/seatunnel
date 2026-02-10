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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;

import lombok.Data;
import lombok.NonNull;

import java.io.File;
import java.io.Serializable;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Data
public class BaseFileSinkConfig implements DelimiterConfig, Serializable {
    private static final long serialVersionUID = 1L;
    protected CompressFormat compressFormat = FileBaseSinkOptions.COMPRESS_CODEC.defaultValue();
    protected String fieldDelimiter;
    protected int sheetMaxRows = FileBaseSinkOptions.SHEET_MAX_ROWS.defaultValue();
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

    public BaseFileSinkConfig(@NonNull ReadonlyConfig config) {
        this.compressFormat = config.get(FileBaseSinkOptions.COMPRESS_CODEC);
        this.batchSize = config.get(FileBaseSinkOptions.BATCH_SIZE);
        this.sheetMaxRows = config.get(FileBaseSinkOptions.SHEET_MAX_ROWS);
        this.rowDelimiter = config.get(FileBaseSinkOptions.ROW_DELIMITER);
        this.path = config.get(FileBaseSinkOptions.FILE_PATH);
        checkNotNull(path);
        if (path.equals(File.separator)) {
            this.path = "";
        }
        this.fileNameExpression = config.get(FileBaseSinkOptions.FILE_NAME_EXPRESSION);
        this.singleFileMode = config.get(FileBaseSinkOptions.SINGLE_FILE_MODE);
        this.createEmptyFileWhenNoData =
                config.get(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA);
        this.fileFormat = config.get(FileBaseSinkOptions.FILE_FORMAT_TYPE);
        if (config.getOptional(FileBaseSinkOptions.FIELD_DELIMITER).isPresent()) {
            this.fieldDelimiter = config.get(FileBaseSinkOptions.FIELD_DELIMITER);
        } else {
            if (FileFormat.CSV.equals(this.fileFormat)) {
                this.fieldDelimiter = ",";
            } else {
                this.fieldDelimiter = FileBaseSinkOptions.FIELD_DELIMITER.defaultValue();
            }
        }
        this.filenameExtension = config.get(FileBaseSinkOptions.FILENAME_EXTENSION);
        dateFormat = config.get(FileBaseSinkOptions.DATE_FORMAT_LEGACY);
        datetimeFormat = config.get(FileBaseSinkOptions.DATETIME_FORMAT_LEGACY);
        timeFormat = config.get(FileBaseSinkOptions.TIME_FORMAT_LEGACY);
        enableHeaderWriter = config.get(FileBaseSinkOptions.ENABLE_HEADER_WRITE);
    }

    public BaseFileSinkConfig() {}
}

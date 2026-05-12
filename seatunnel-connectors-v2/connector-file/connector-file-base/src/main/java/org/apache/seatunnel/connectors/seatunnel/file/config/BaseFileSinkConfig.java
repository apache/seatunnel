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
    protected CompressFormat compressFormat;
    protected String fieldDelimiter;
    protected int sheetMaxRows;
    protected String rowDelimiter;
    protected int batchSize;
    protected String path;
    protected String fileNameExpression;
    protected boolean customFilename;
    protected boolean singleFileMode;
    protected boolean createEmptyFileWhenNoData;
    protected FileFormat fileFormat;
    protected String filenameExtension;
    protected DateUtils.Formatter dateFormat;
    protected DateTimeUtils.Formatter datetimeFormat;
    protected TimeUtils.Formatter timeFormat;
    protected Boolean enableHeaderWriter = false;

    public BaseFileSinkConfig(@NonNull ReadonlyConfig pluginConfig) {
        this.compressFormat = pluginConfig.get(FileBaseSinkOptions.COMPRESS_CODEC);
        this.batchSize = pluginConfig.get(FileBaseSinkOptions.BATCH_SIZE);
        this.sheetMaxRows = pluginConfig.get(FileBaseSinkOptions.SHEET_MAX_ROWS);
        this.rowDelimiter = pluginConfig.get(FileBaseSinkOptions.ROW_DELIMITER);
        this.path = pluginConfig.get(FileBaseSinkOptions.FILE_PATH);
        checkNotNull(path);
        if (path.equals(File.separator)) {
            this.path = "";
        }
        this.fileNameExpression = pluginConfig.get(FileBaseSinkOptions.FILE_NAME_EXPRESSION);
        this.customFilename = pluginConfig.get(FileBaseSinkOptions.CUSTOM_FILENAME);
        this.singleFileMode = pluginConfig.get(FileBaseSinkOptions.SINGLE_FILE_MODE);
        this.createEmptyFileWhenNoData =
                pluginConfig.get(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA);
        this.fileFormat = pluginConfig.get(FileBaseSinkOptions.FILE_FORMAT_TYPE);
        // if set, use user config, if not set, when format is csv, use "," otherwise use default
        // delimiter
        if (pluginConfig.getOptional(FileBaseSinkOptions.FIELD_DELIMITER).isPresent()) {
            this.fieldDelimiter = pluginConfig.get(FileBaseSinkOptions.FIELD_DELIMITER);
        } else if (FileFormat.CSV.equals(this.fileFormat)) {
            this.fieldDelimiter = ",";
        } else {
            this.fieldDelimiter = FileBaseSinkOptions.FIELD_DELIMITER.defaultValue();
        }
        this.filenameExtension = pluginConfig.get(FileBaseSinkOptions.FILENAME_EXTENSION);
        this.dateFormat = pluginConfig.get(FileBaseSinkOptions.DATE_FORMAT_LEGACY);
        this.datetimeFormat = pluginConfig.get(FileBaseSinkOptions.DATETIME_FORMAT_LEGACY);
        this.timeFormat = pluginConfig.get(FileBaseSinkOptions.TIME_FORMAT_LEGACY);
        this.enableHeaderWriter = pluginConfig.get(FileBaseSinkOptions.ENABLE_HEADER_WRITE);
    }

    public BaseFileSinkConfig() {}
}

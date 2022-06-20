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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Locale;

@Data
public class AbstractTextFileConfig implements DelimiterConfig, CompressConfig, Serializable {
    protected String compressCodec;

    protected String fieldDelimiter = String.valueOf('\001');

    protected String rowDelimiter = "\n";

    protected String path;
    protected String fileNameExpression;
    protected FileFormat fileFormat = FileFormat.TEXT;

    public AbstractTextFileConfig(@NonNull Config config) {
        checkNotNull(config.getString(Constant.PATH));

        if (config.hasPath(Constant.COMPRESS_CODEC)) {
            throw new RuntimeException("compress not support now");
        }

        if (config.hasPath(Constant.FIELD_DELIMITER) && !StringUtils.isBlank(config.getString(Constant.FIELD_DELIMITER))) {
            this.fieldDelimiter = config.getString(Constant.FIELD_DELIMITER);
        }

        if (config.hasPath(Constant.ROW_DELIMITER) && !StringUtils.isBlank(config.getString(Constant.ROW_DELIMITER))) {
            this.rowDelimiter = config.getString(Constant.ROW_DELIMITER);
        }

        if (config.hasPath(Constant.PATH) && !StringUtils.isBlank(config.getString(Constant.PATH))) {
            this.path = config.getString(Constant.PATH);
        }

        if (config.hasPath(Constant.FILE_NAME_EXPRESSION) && !StringUtils.isBlank(config.getString(Constant.FILE_NAME_EXPRESSION))) {
            this.fileNameExpression = config.getString(Constant.FILE_NAME_EXPRESSION);
        }

        if (config.hasPath(Constant.FILE_FORMAT) && !StringUtils.isBlank(config.getString(Constant.FILE_FORMAT))) {
            this.fileFormat = FileFormat.valueOf(config.getString(Constant.FILE_FORMAT).toUpperCase(Locale.ROOT));
        }
    }

    protected AbstractTextFileConfig() {
    }
}

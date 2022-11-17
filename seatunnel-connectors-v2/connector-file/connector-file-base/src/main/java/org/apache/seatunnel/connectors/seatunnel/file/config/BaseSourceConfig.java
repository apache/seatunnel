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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;

public class BaseSourceConfig {
    public static final Option<String> FILE_TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("File type");
    public static final Option<FileFormat> FILE_PATH = Options.key("path")
            .enumType(FileFormat.class)
            .noDefaultValue()
            .withDescription("The file path of source files");
    public static final Option<String> DELIMITER = Options.key("delimiter")
            .stringType()
            .defaultValue(String.valueOf('\001'))
            .withDescription("The separator between columns in a row of data. Only needed by `text` and `csv` file format");
    public static final Option<DateUtils.Formatter> DATE_FORMAT = Options.key("date_format")
            .enumType(DateUtils.Formatter.class)
            .defaultValue(DateUtils.Formatter.YYYY_MM_DD)
            .withDescription("Date format");
    public static final Option<DateTimeUtils.Formatter> DATETIME_FORMAT = Options.key("datetime_format")
            .enumType(DateTimeUtils.Formatter.class)
            .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
            .withDescription("Datetime format");
    public static final Option<TimeUtils.Formatter> TIME_FORMAT = Options.key("time_format")
            .enumType(TimeUtils.Formatter.class)
            .defaultValue(TimeUtils.Formatter.HH_MM_SS)
            .withDescription("Time format");
    public static final Option<Boolean> PARSE_PARTITION_FROM_PATH = Options.key("parse_partition_from_path")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether parse partition fields from file path");
}

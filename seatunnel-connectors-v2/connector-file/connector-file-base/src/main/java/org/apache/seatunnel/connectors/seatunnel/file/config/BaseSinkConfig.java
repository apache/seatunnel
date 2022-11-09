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

import java.util.List;

public class BaseSinkConfig {
    public static final String SEATUNNEL = "seatunnel";
    public static final String NON_PARTITION = "NON_PARTITION";
    public static final String TRANSACTION_ID_SPLIT = "_";
    public static final String TRANSACTION_EXPRESSION = "transactionId";
    public static final Option<String> COMPRESS_CODEC = Options.key("compress_codec")
            .stringType()
            .noDefaultValue()
            .withDescription("Compression codec");
    public static final Option<String> DATE_FORMAT = Options.key("date_format")
            .stringType()
            .defaultValue(DateUtils.Formatter.YYYY_MM_DD.getValue())
            .withDescription("Date format");
    public static final Option<String> DATETIME_FORMAT = Options.key("datetime_format")
            .stringType()
            .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS.getValue())
            .withDescription("Datetime format");
    public static final Option<String> TIME_FORMAT = Options.key("time_format")
            .stringType()
            .defaultValue(TimeUtils.Formatter.HH_MM_SS.getValue())
            .withDescription("Time format");
    public static final Option<String> FILE_PATH = Options.key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("File path");
    public static final Option<String> FIELD_DELIMITER = Options.key("field_delimiter")
            .stringType()
            .defaultValue(String.valueOf(String.valueOf('\001')))
            .withDescription("Field delimiter");
    public static final Option<String> ROW_DELIMITER = Options.key("row_delimiter")
            .stringType()
            .defaultValue("\n")
            .withDescription("Row delimiter");
    public static final Option<List<String>> PARTITION_BY = Options.key("partition_by")
            .listType()
            .noDefaultValue()
            .withDescription("Partition keys list");
    public static final Option<String> PARTITION_DIR_EXPRESSION = Options.key("partition_dir_expression")
            .stringType()
            .defaultValue("${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/")
            .withDescription("Partition directory expressions");
    public static final Option<Boolean> IS_PARTITION_FIELD_WRITE_IN_FILE = Options.key("is_partition_field_write_in_file")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to write partition fields to file");
    public static final Option<String> TMP_PATH = Options.key("tmp_path")
            .stringType()
            .defaultValue("/tmp/seatunnel")
            .withDescription("Data write temporary path");
    public static final Option<String> FILE_NAME_EXPRESSION = Options.key("file_name_expression")
            .stringType()
            .defaultValue("${transactionId}")
            .withDescription("Describe the file expression which will be created into the path");
    public static final Option<String> FILE_FORMAT = Options.key("file_format")
            .stringType()
            .defaultValue(FileFormat.TEXT.toString())
            .withDescription("File format type");
    public static final Option<List<String>> SINK_COLUMNS = Options.key("sink_columns")
            .listType()
            .noDefaultValue()
            .withDescription("Which columns need be write to file");
    public static final Option<String> FILENAME_TIME_FORMAT = Options.key("filename_time_format")
            .stringType()
            .defaultValue(DateUtils.Formatter.YYYY_MM_DD_SPOT.getValue())
            .withDescription("The time format of the path");
    public static final Option<Boolean> IS_ENABLE_TRANSACTION = Options.key("is_enable_transaction")
            .booleanType()
            .defaultValue(true)
            .withDescription("If or not enable transaction");
}

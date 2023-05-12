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
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import java.util.Arrays;
import java.util.List;

public class BaseSinkConfig {
    public static final String SEATUNNEL = "seatunnel";
    public static final String NON_PARTITION = "NON_PARTITION";
    public static final String TRANSACTION_ID_SPLIT = "_";
    public static final String TRANSACTION_EXPRESSION = "transactionId";
    public static final String DEFAULT_FIELD_DELIMITER = TextFormatConstant.SEPARATOR[0];
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final String DEFAULT_PARTITION_DIR_EXPRESSION =
            "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/";
    public static final String DEFAULT_TMP_PATH = "/tmp/seatunnel";
    public static final String DEFAULT_FILE_NAME_EXPRESSION = "${transactionId}";
    public static final int DEFAULT_BATCH_SIZE = 1000000;

    public static final Option<CompressFormat> COMPRESS_CODEC =
            Options.key("compress_codec")
                    .enumType(CompressFormat.class)
                    .defaultValue(CompressFormat.NONE)
                    .withDescription("Compression codec");

    public static final Option<CompressFormat> TXT_COMPRESS =
            Options.key("compress_codec")
                    .singleChoice(
                            CompressFormat.class,
                            Arrays.asList(CompressFormat.NONE, CompressFormat.LZO))
                    .defaultValue(CompressFormat.NONE)
                    .withDescription("Txt file supported compression");

    public static final Option<CompressFormat> PARQUET_COMPRESS =
            Options.key("compress_codec")
                    .singleChoice(
                            CompressFormat.class,
                            Arrays.asList(
                                    CompressFormat.NONE,
                                    CompressFormat.LZO,
                                    CompressFormat.SNAPPY,
                                    CompressFormat.LZ4,
                                    CompressFormat.GZIP,
                                    CompressFormat.BROTLI,
                                    CompressFormat.ZSTD))
                    .defaultValue(CompressFormat.NONE)
                    .withDescription("Parquet file supported compression");

    public static final Option<CompressFormat> ORC_COMPRESS =
            Options.key("compress_codec")
                    .singleChoice(
                            CompressFormat.class,
                            Arrays.asList(
                                    CompressFormat.NONE,
                                    CompressFormat.LZO,
                                    CompressFormat.SNAPPY,
                                    CompressFormat.LZ4,
                                    CompressFormat.ZLIB))
                    .defaultValue(CompressFormat.NONE)
                    .withDescription("Orc file supported compression");

    public static final Option<DateUtils.Formatter> DATE_FORMAT =
            Options.key("date_format")
                    .enumType(DateUtils.Formatter.class)
                    .defaultValue(DateUtils.Formatter.YYYY_MM_DD)
                    .withDescription("Date format");

    public static final Option<DateTimeUtils.Formatter> DATETIME_FORMAT =
            Options.key("datetime_format")
                    .enumType(DateTimeUtils.Formatter.class)
                    .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                    .withDescription("Datetime format");

    public static final Option<TimeUtils.Formatter> TIME_FORMAT =
            Options.key("time_format")
                    .enumType(TimeUtils.Formatter.class)
                    .defaultValue(TimeUtils.Formatter.HH_MM_SS)
                    .withDescription("Time format");

    public static final Option<String> FILE_PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of target files");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_FIELD_DELIMITER)
                    .withDescription(
                            "The separator between columns in a row of data. Only needed by `text` and `csv` file format");

    public static final Option<String> ROW_DELIMITER =
            Options.key("row_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_ROW_DELIMITER)
                    .withDescription(
                            "The separator between rows in a file. Only needed by `text` and `csv` file format");

    public static final Option<Boolean> HAVE_PARTITION =
            Options.key("have_partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether need partition when write data");

    public static final Option<List<String>> PARTITION_BY =
            Options.key("partition_by")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Partition keys list, Only used when have_partition is true");

    public static final Option<String> PARTITION_DIR_EXPRESSION =
            Options.key("partition_dir_expression")
                    .stringType()
                    .defaultValue(DEFAULT_PARTITION_DIR_EXPRESSION)
                    .withDescription(
                            "Only used when have_partition is true. If the `partition_by` is specified, "
                                    + "we will generate the corresponding partition directory based on the partition information, "
                                    + "and the final file will be placed in the partition directory. "
                                    + "Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. "
                                    + "`k0` is the first partition field and `v0` is the value of the first partition field.");

    public static final Option<Boolean> IS_PARTITION_FIELD_WRITE_IN_FILE =
            Options.key("is_partition_field_write_in_file")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Only used when have_partition is true. Whether to write partition fields to file");

    public static final Option<String> TMP_PATH =
            Options.key("tmp_path")
                    .stringType()
                    .defaultValue(DEFAULT_TMP_PATH)
                    .withDescription("Data write temporary path");

    public static final Option<Boolean> CUSTOM_FILENAME =
            Options.key("custom_filename")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether custom the output filename");

    public static final Option<String> FILE_NAME_EXPRESSION =
            Options.key("file_name_expression")
                    .stringType()
                    .defaultValue(DEFAULT_FILE_NAME_EXPRESSION)
                    .withDescription(
                            "Only used when `custom_filename` is true. `file_name_expression` describes the file expression which will be created into the `path`. "
                                    + "We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, "
                                    + "like `test_${uuid}_${now}`,`${now}` represents the current time, "
                                    + "and its format can be defined by specifying the option `filename_time_format`.");

    public static final Option<String> FILENAME_TIME_FORMAT =
            Options.key("filename_time_format")
                    .stringType()
                    .defaultValue(DateUtils.Formatter.YYYY_MM_DD_SPOT.getValue())
                    .withDescription(
                            "Only used when `custom_filename` is true. The time format of the path");

    public static final Option<FileFormat> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .enumType(FileFormat.class)
                    .defaultValue(FileFormat.CSV)
                    .withDescription("File format type, e.g. csv, orc, parquet, text");

    public static final Option<List<String>> SINK_COLUMNS =
            Options.key("sink_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Which columns need be wrote to file");

    public static final Option<Boolean> IS_ENABLE_TRANSACTION =
            Options.key("is_enable_transaction")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("If or not enable transaction");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SIZE)
                    .withDescription("The batch size of each split file");

    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hdfs-site.xml");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos principal");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos keytab file path");

    public static final Option<Integer> MAX_ROWS_IN_MEMORY =
            Options.key("max_rows_in_memory")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Max rows in memory,only valid for excel files");

    public static final Option<String> SHEET_NAME =
            Options.key("sheet_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("To be written sheet name,only valid for excel files");
}

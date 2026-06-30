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
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FileBaseSourceOptions extends FileBaseOptions {
    public static final String DEFAULT_ROW_DELIMITER = "\n";

    public static final Option<FileDiscoveryMode> DISCOVERY_MODE =
            Options.key("discovery_mode")
                    .singleChoice(
                            FileDiscoveryMode.class,
                            Arrays.asList(FileDiscoveryMode.ONCE, FileDiscoveryMode.CONTINUOUS))
                    .defaultValue(FileDiscoveryMode.ONCE)
                    .withDescription(
                            "File discovery mode. Supported values: once (default), continuous. "
                                    + "When set to continuous, the source keeps scanning the path and processes new/changed files at runtime.");

    public static final Option<Duration> SCAN_INTERVAL =
            Options.key("scan_interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Scan interval for discovery_mode=continuous. Recommended shorthand format is 10S; ISO-8601 format PT10S is also supported. Default is 10S.");

    public static final Option<FileStartMode> START_MODE =
            Options.key("start_mode")
                    .singleChoice(
                            FileStartMode.class,
                            Arrays.asList(FileStartMode.EARLIEST, FileStartMode.LATEST))
                    .defaultValue(FileStartMode.EARLIEST)
                    .withDescription(
                            "Start mode for discovery_mode=continuous. Supported values: earliest (default), latest. "
                                    + "earliest reads existing files on startup; latest only processes files modified after the job starts.");

    public static final Option<FileFormat> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .objectType(FileFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "File format type, e.g. json, csv, text, parquet, orc, avro....");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(TextFormatConstant.SEPARATOR[0])
                    .withFallbackKeys("delimiter")
                    .withDescription(
                            "The separator between columns in a row of data. Only needed by `text` file format");

    public static final Option<String> ROW_DELIMITER =
            Options.key("row_delimiter")
                    .stringType()
                    .defaultValue(DEFAULT_ROW_DELIMITER)
                    .withDescription(
                            "The separator between rows in a file. Only needed by `text` file format");

    public static final Option<String> NULL_FORMAT =
            Options.key("null_format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The string that represents a null value");

    public static final Option<Boolean> PARSE_PARTITION_FROM_PATH =
            Options.key("parse_partition_from_path")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether parse partition fields from file path");

    public static final Option<Long> SKIP_HEADER_ROW_NUMBER =
            Options.key("skip_header_row_number")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("The number of rows to skip");

    public static final Option<List<String>> READ_PARTITIONS =
            Options.key("read_partitions")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The partitions that the user want to read");

    public static final Option<List<String>> READ_COLUMNS =
            Options.key("read_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The columns list that the user want to read");

    public static final Option<Boolean> MARKDOWN_RAG_METADATA_ENABLED =
            Options.key("markdown_rag_metadata_enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to append RAG-oriented metadata columns when reading markdown files. "
                                    + "Only valid when file_format_type is markdown.");

    public static final Option<ExcelEngine> EXCEL_ENGINE =
            Options.key("excel_engine")
                    .enumType(ExcelEngine.class)
                    .defaultValue(ExcelEngine.POI)
                    .withDescription("To switch excel read engine,  e.g. POI , EasyExcel");

    public static final Option<String> XML_ROW_TAG =
            Options.key("xml_row_tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the tag name of the data rows within the XML file, only valid for XML files.");

    public static final Option<String> FILE_FILTER_PATTERN =
            Options.key("file_filter_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File pattern. The connector will filter some files base on the pattern.");

    public static final Option<String> FILE_FILTER_MODIFIED_START =
            Options.key("file_filter_modified_start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File modification time filter. The connector will filter some files base on the last modification start time (include start time). the default data format is yyyy-MM-dd HH:mm:ss");

    public static final Option<String> FILE_FILTER_MODIFIED_END =
            Options.key("file_filter_modified_end")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File modification time filter. The connector will filter some files base on the last modification end time (not include end time). the default data format is yyyy-MM-dd HH:mm:ss");

    public static final Option<Integer> BINARY_CHUNK_SIZE =
            Options.key("binary_chunk_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The chunk size (in bytes) for reading binary files. Default is 1024 bytes. "
                                    + "Larger values may improve performance for large files but use more memory.Only valid when file_format_type is binary.");

    public static final Option<Boolean> BINARY_COMPLETE_FILE_MODE =
            Options.key("binary_complete_file_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read the complete file as a single chunk instead of splitting into chunks. "
                                    + "When enabled, the entire file content will be read into memory at once.Only valid when file_format_type is binary.");

    public static final Option<FileSyncMode> SYNC_MODE =
            Options.key("sync_mode")
                    .singleChoice(
                            FileSyncMode.class,
                            Arrays.asList(FileSyncMode.FULL, FileSyncMode.UPDATE))
                    .defaultValue(FileSyncMode.FULL)
                    .withDescription(
                            "File sync mode. Supported values: full, update. "
                                    + "When set to update, the source will compare with target and only read new/changed files. "
                                    + "Currently, update mode only supports file_format_type=binary.");

    public static final Option<String> TARGET_PATH =
            Options.key("target_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target base path for sync_mode=update comparison.");

    public static final Option<Map<String, String>> TARGET_HADOOP_CONF =
            Options.key("target_hadoop_conf")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Extra Hadoop configuration for target filesystem in sync_mode=update. "
                                    + "Use key 'fs.defaultFS' to override target defaultFS if needed.");

    public static final Option<FileUpdateStrategy> UPDATE_STRATEGY =
            Options.key("update_strategy")
                    .singleChoice(
                            FileUpdateStrategy.class,
                            Arrays.asList(FileUpdateStrategy.DISTCP, FileUpdateStrategy.STRICT))
                    .defaultValue(FileUpdateStrategy.DISTCP)
                    .withDescription(
                            "Update strategy when sync_mode=update. Supported values: distcp, strict. "
                                    + "distcp behaves like 'distcp -update' (len+mtime, and does not require equal mtime). "
                                    + "strict requires exact consistency depending on compare_mode.");

    public static final Option<FileCompareMode> COMPARE_MODE =
            Options.key("compare_mode")
                    .singleChoice(
                            FileCompareMode.class,
                            Arrays.asList(FileCompareMode.LEN_MTIME, FileCompareMode.CHECKSUM))
                    .defaultValue(FileCompareMode.LEN_MTIME)
                    .withDescription(
                            "Compare mode when sync_mode=update. Supported values: len_mtime, checksum. "
                                    + "checksum uses Hadoop FileSystem#getFileChecksum, only valid when update_strategy=strict.");
    public static final Option<String> QUOTE_CHAR =
            Options.key("quote_char")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription(
                            "A single character that encloses CSV fields, allowing fields with commas, line breaks, or quotes to be read correctly.");

    public static final Option<String> ESCAPE_CHAR =
            Options.key("escape_char")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A single character that allows the quote or other special characters to appear inside a CSV field without ending the field.");

    public static final Option<Boolean> RECURSIVE_FILE_SCAN =
            Options.key("recursive_file_scan")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to recursively scan subdirectories. "
                                    + "If false, subdirectories will be ignored.");

    public static final Option<Boolean> SORT_FILES_BY_MOD_TIME =
            Options.key("sort_files_by_modification_time")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Sort files by modification time in descending order. "
                                    + "Enable this when reading evolving schemas to ensure schema inference uses the latest file. "
                                    + "Disabled by default to avoid performance overhead for large file directories.");
}

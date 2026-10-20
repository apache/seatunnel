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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.format.csv.CsvDeserializationSchema;
import org.apache.seatunnel.format.csv.processor.CsvLineProcessor;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVFormat.Builder;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CsvReadStrategy extends AbstractReadStrategy {
    private CsvDeserializationSchema deserializationSchema;
    private DateUtils.Formatter dateFormat =
            FileBaseSourceOptions.DATE_FORMAT_LEGACY.defaultValue();
    private DateTimeUtils.Formatter datetimeFormat =
            FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.defaultValue();
    private TimeUtils.Formatter timeFormat =
            FileBaseSourceOptions.TIME_FORMAT_LEGACY.defaultValue();
    private CompressFormat compressFormat = FileBaseSourceOptions.COMPRESS_CODEC.defaultValue();
    private CsvLineProcessor processor;
    private int[] indexes;
    private String encoding = FileBaseSourceOptions.ENCODING.defaultValue();
    private CatalogTable inputCatalogTable;
    private boolean firstLineAsHeader = FileBaseSourceOptions.CSV_USE_HEADER_LINE.defaultValue();

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(
                new FileSourceSplit(tableId, path), output, partitionsMap, FileFormat.CSV);
    }

    @Override
    public void read(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        Map<String, String> partitionsMap = parsePartitionsByPath(split.getFilePath());
        resolveArchiveCompressedInputStream(split, output, partitionsMap, FileFormat.CSV);
    }

    @Override
    public void readProcess(
            FileSourceSplit split,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {
        log.info(
                "Start reading CSV file: {}, split start: {}, split length: {}",
                currentFileName,
                split.getStart(),
                split.getLength());
        final boolean useSplitRead = isSplitReadEnabled(split);
        try (BufferedReader reader =
                        createBomAwareBufferedReader(
                                wrapInputStream(inputStream, split), encoding);
                CSVParser csvParser = new CSVParser(reader, getCSVFormat(split))) {
            // skip lines
            // if split range is used, no need to skip
            if (!useSplitRead) {
                for (int i = 0; i < skipHeaderNumber; i++) {
                    if (reader.readLine() == null) {
                        throw new IOException(
                                String.format(
                                        "File [%s] has fewer lines than expected to skip.",
                                        currentFileName));
                    }
                }
            }
            // read header lines
            List<String> headers = getHeaders(csvParser, split);
            // Clean up BOM characters (\uFEFF) in the header to solve occasional BOM residue
            // issues
            List<String> cleanedHeaders =
                    headers.stream()
                            .map(header -> header.replace("\uFEFF", ""))
                            .collect(Collectors.toList());
            for (CSVRecord csvRecord : csvParser) {
                HashMap<Integer, String> fieldIdValueMap = new HashMap<>();
                for (int i = 0; i < cleanedHeaders.size(); i++) {
                    // the user input schema may not contain all the columns in the csv header
                    // and may contain columns in a different order with the csv header
                    int index =
                            inputCatalogTable
                                    .getSeaTunnelRowType()
                                    .indexOf(cleanedHeaders.get(i), false);
                    if (index == -1) {
                        continue;
                    }
                    fieldIdValueMap.put(index, csvRecord.get(i));
                }
                SeaTunnelRow seaTunnelRow = deserializationSchema.getSeaTunnelRow(fieldIdValueMap);
                if (!readColumns.isEmpty()) {
                    // need column projection
                    Object[] fields;
                    if (isMergePartition) {
                        fields = new Object[readColumns.size() + partitionsMap.size()];
                    } else {
                        fields = new Object[readColumns.size()];
                    }
                    for (int i = 0; i < indexes.length; i++) {
                        fields[i] = seaTunnelRow.getField(indexes[i]);
                    }
                    seaTunnelRow = new SeaTunnelRow(fields);
                }
                if (isMergePartition) {
                    int index = seaTunnelRowType.getTotalFields();
                    for (String value : partitionsMap.values()) {
                        seaTunnelRow.setField(index++, value);
                    }
                }
                seaTunnelRow.setTableId(split.getTableId());
                output.collect(seaTunnelRow);
            }
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Deserialize this file [%s] failed, please check the origin data",
                            currentFileName);
            throw new FileConnectorException(
                    FileConnectorErrorCode.DATA_DESERIALIZE_FAILED, errorMsg, e);
        }
    }

    private InputStream wrapInputStream(InputStream inputStream, FileSourceSplit split)
            throws IOException {
        InputStream resultStream;
        // process compression isnputStream
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                resultStream = lzo.createInputStream(inputStream);
                break;
            case NONE:
                resultStream = inputStream;
                break;
            default:
                log.warn(
                        "Csv file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                resultStream = inputStream;
                break;
        }
        // rebuild inputStream
        if (isSplitReadEnabled(split)) {
            resultStream = safeSlice(resultStream, split.getStart(), split.getLength());
        }
        return resultStream;
    }

    private boolean isSplitReadEnabled(FileSourceSplit split) {
        return enableSplitFile && split.getLength() > -1;
    }

    private CSVFormat getCSVFormat(FileSourceSplit split) {
        String quoteChar = readonlyConfig.get(FileBaseSourceOptions.QUOTE_CHAR);
        String escapeChar = readonlyConfig.get(FileBaseSourceOptions.ESCAPE_CHAR);
        Builder builder =
                CSVFormat.EXCEL.builder().setIgnoreEmptyLines(true).setDelimiter(getDelimiter());
        if (StringUtils.isNotEmpty(quoteChar)) {
            builder.setQuote(quoteChar.charAt(0));
        }
        if (StringUtils.isNotEmpty(escapeChar)) {
            builder.setEscape(escapeChar.charAt(0));
        }
        CSVFormat csvFormat = builder.build();
        final boolean useSplitRead = isSplitReadEnabled(split);
        // if split range is used, header should only be read in the first split
        if (firstLineAsHeader && (!useSplitRead || split.getStart() == 0)) {
            csvFormat = csvFormat.withFirstRecordAsHeader();
        }
        return csvFormat;
    }

    private List<String> getHeaders(CSVParser csvParser, FileSourceSplit split) {
        List<String> headers;
        final boolean useSplitRead = isSplitReadEnabled(split);
        if (firstLineAsHeader && (!useSplitRead || split.getStart() == 0)) {
            headers = new ArrayList<>(csvParser.getHeaderNames());
        } else {
            headers =
                    inputCatalogTable.getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .collect(Collectors.toList());
        }
        return headers;
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) {
        this.seaTunnelRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(getPathForPartitionInference(path), seaTunnelRowType);
        initFormatter();
        if (pluginConfig.hasPath(FileBaseSourceOptions.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading csv files, if user has not specified schema information, "
                            + "SeaTunnel will not support column projection");
        }
        CsvDeserializationSchema.Builder builder =
                CsvDeserializationSchema.builder()
                        .delimiter(getDelimiter())
                        .csvLineProcessor(processor)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(FileBaseSourceOptions.NULL_FORMAT)
                                        .orElse(null));
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(this.seaTunnelRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(this.seaTunnelRowType).build();
        }
        return getActualSeaTunnelRowTypeInfo();
    }

    private String getDelimiter() {
        return readonlyConfig.getOptional(FileBaseSourceOptions.FIELD_DELIMITER).orElse(",");
    }

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        this.inputCatalogTable = catalogTable;
        String partitionPath = getPathForPartitionInference(null);
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(partitionPath, rowType);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        encoding =
                readonlyConfig
                        .getOptional(FileBaseSourceOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        initFormatter();
        CsvDeserializationSchema.Builder builder =
                CsvDeserializationSchema.builder()
                        .delimiter(getDelimiter())
                        .csvLineProcessor(processor)
                        .nullFormat(
                                readonlyConfig
                                        .getOptional(FileBaseSourceOptions.NULL_FORMAT)
                                        .orElse(null));
        if (pluginConfig.hasPath(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key())) {
            firstLineAsHeader =
                    pluginConfig.getBoolean(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key());
        }
        if (isMergePartition) {
            deserializationSchema =
                    builder.seaTunnelRowType(userDefinedRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.seaTunnelRowType(rowType).build();
        }
        // column projection
        if (pluginConfig.hasPath(FileBaseSourceOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = rowType.indexOf(readColumns.get(i));
                fields[i] = rowType.getFieldName(indexes[i]);
                types[i] = rowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(partitionPath, this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = rowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    private void initFormatter() {
        if (pluginConfig.hasPath(FileBaseSourceOptions.DATE_FORMAT_LEGACY.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            pluginConfig.getString(FileBaseSourceOptions.DATE_FORMAT_LEGACY.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            pluginConfig.getString(
                                    FileBaseSourceOptions.DATETIME_FORMAT_LEGACY.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.TIME_FORMAT_LEGACY.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            pluginConfig.getString(FileBaseSourceOptions.TIME_FORMAT_LEGACY.key()));
        }
        if (pluginConfig.hasPath(FileBaseSourceOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(FileBaseSourceOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }

        processor = new DefaultCsvLineProcessor();
    }
}

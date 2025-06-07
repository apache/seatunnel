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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
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
import org.apache.seatunnel.format.csv.CsvDeserializationSchema;
import org.apache.seatunnel.format.csv.processor.CsvLineProcessor;
import org.apache.seatunnel.format.csv.processor.DefaultCsvLineProcessor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.CSV);
    }

    @Override
    public void readProcess(
            String path,
            String tableId,
            Collector<SeaTunnelRow> output,
            InputStream inputStream,
            Map<String, String> partitionsMap,
            String currentFileName)
            throws IOException {
        InputStream actualInputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                actualInputStream = lzo.createInputStream(inputStream);
                break;
            case NONE:
                actualInputStream = inputStream;
                break;
            default:
                log.warn(
                        "Csv file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                actualInputStream = inputStream;
                break;
        }

        CSVFormat csvFormat = CSVFormat.DEFAULT;
        if (firstLineAsHeader) {
            csvFormat = csvFormat.withFirstRecordAsHeader();
        }
        try (BufferedReader reader =
                        new BufferedReader(new InputStreamReader(actualInputStream, encoding));
                CSVParser csvParser = new CSVParser(reader, csvFormat); ) {
            // test and skip `\uFEFF` BOM
            reader.mark(1);
            int firstChar = reader.read();
            if (firstChar != 0xFEFF) {
                reader.reset();
            }
            // skip lines
            for (int i = 0; i < skipHeaderNumber; i++) {
                if (reader.readLine() == null) {
                    throw new IOException(
                            String.format(
                                    "File [%s] has fewer lines than expected to skip.",
                                    currentFileName));
                }
            }
            // read lines
            List<String> headers = getHeaders(csvParser);
            for (CSVRecord csvRecord : csvParser) {
                HashMap<Integer, String> fieldIdValueMap = new HashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    // the user input schema may not contain all the columns in the csv header
                    // and may contain columns in a different order with the csv header
                    int index =
                            inputCatalogTable.getSeaTunnelRowType().indexOf(headers.get(i), false);
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
                seaTunnelRow.setTableId(tableId);
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

    private List<String> getHeaders(CSVParser csvParser) {
        List<String> headers;
        if (firstLineAsHeader) {
            headers = csvParser.getHeaderNames().stream().collect(Collectors.toList());
        } else {
            headers =
                    inputCatalogTable.getTableSchema().getColumns().stream()
                            .map(column -> column.getName())
                            .collect(Collectors.toList());
        }
        return headers;
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) {
        this.seaTunnelRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.seaTunnelRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        initFormatter();
        if (pluginConfig.hasPath(FileBaseSourceOptions.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading csv files, if user has not specified schema information, "
                            + "SeaTunnel will not support column projection");
        }
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        CsvDeserializationSchema.Builder builder =
                CsvDeserializationSchema.builder()
                        .delimiter(",")
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

    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        this.inputCatalogTable = catalogTable;
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), rowType);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        encoding =
                readonlyConfig
                        .getOptional(FileBaseSourceOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        initFormatter();
        CsvDeserializationSchema.Builder builder =
                CsvDeserializationSchema.builder()
                        .delimiter(",")
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
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
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

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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import lombok.SneakyThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.seatunnel.common.utils.DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;

public class ExcelReadStrategy extends AbstractReadStrategy {

    private final DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;

    private final DateTimeUtils.Formatter datetimeFormat = YYYY_MM_DD_HH_MM_SS;
    private final TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    private int[] indexes;

    private int cellCount;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public void read(String path, Collector<SeaTunnelRow> output) {
        Configuration conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        Path filePath = new Path(path);
        FSDataInputStream file = fs.open(filePath);
        Workbook workbook = new XSSFWorkbook(file);
        Sheet sheet =
                pluginConfig.hasPath(BaseSourceConfig.SHEET_NAME.key())
                        ? workbook.getSheet(
                                pluginConfig.getString(BaseSourceConfig.SHEET_NAME.key()))
                        : workbook.getSheetAt(0);
        cellCount = seaTunnelRowType.getTotalFields();
        cellCount = partitionsMap.isEmpty() ? cellCount : cellCount + partitionsMap.size();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(cellCount);
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        int rowCount = sheet.getPhysicalNumberOfRows();
        if (skipHeaderNumber > Integer.MAX_VALUE
                || skipHeaderNumber < Integer.MIN_VALUE
                || skipHeaderNumber > rowCount) {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
        }
        IntStream.range((int) skipHeaderNumber, rowCount)
                .mapToObj(sheet::getRow)
                .filter(Objects::nonNull)
                .forEach(
                        rowData -> {
                            int[] cellIndexes =
                                    indexes == null
                                            ? IntStream.range(0, cellCount).toArray()
                                            : indexes;
                            int z = 0;
                            for (int j : cellIndexes) {
                                Cell cell = rowData.getCell(j);
                                seaTunnelRow.setField(
                                        z++,
                                        cell == null
                                                ? null
                                                : convert(
                                                        getCellValue(cell.getCellType(), cell),
                                                        fieldTypes[z - 1]));
                            }
                            if (isMergePartition) {
                                int index = seaTunnelRowType.getTotalFields();
                                for (String value : partitionsMap.values()) {
                                    seaTunnelRow.setField(index++, value);
                                }
                            }
                            output.collect(seaTunnelRow);
                        });
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        if (isNullOrEmpty(seaTunnelRowType.getFieldNames())
                || isNullOrEmpty(seaTunnelRowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Schmea information is not set or incorrect schmea settings");
        }
        SeaTunnelRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfig.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = seaTunnelRowType.indexOf(readColumns.get(i));
                fields[i] = seaTunnelRowType.getFieldName(indexes[i]);
                types[i] = seaTunnelRowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        } else {
            this.seaTunnelRowType = seaTunnelRowType;
            this.seaTunnelRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    Configuration getConfiguration() {
        return getConfiguration(hadoopConf);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path)
            throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }

    private Object getCellValue(CellType cellType, Cell cell) {
        switch (cellType) {
            case STRING:
                return cell.getStringCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    DataFormatter formatter = new DataFormatter();
                    return formatter.formatCellValue(cell);
                }
                return cell.getNumericCellValue();
            case ERROR:
                break;
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("[%s] type not support ", cellType));
        }
        return null;
    }

    @SneakyThrows
    private Object convert(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case MAP:
            case ARRAY:
                return objectMapper.readValue((String) field, fieldType.getTypeClass());
            case STRING:
                return field;
            case DOUBLE:
                return Double.parseDouble(field.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(field.toString());
            case FLOAT:
                return (float) Double.parseDouble(field.toString());
            case BIGINT:
                return (long) Double.parseDouble(field.toString());
            case INT:
                return (int) Double.parseDouble(field.toString());
            case TINYINT:
                return (byte) Double.parseDouble(field.toString());
            case SMALLINT:
                return (short) Double.parseDouble(field.toString());
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(field.toString()));
            case DATE:
                return LocalDate.parse(
                        (String) field, DateTimeFormatter.ofPattern(dateFormat.getValue()));
            case TIME:
                return LocalTime.parse(
                        (String) field, DateTimeFormatter.ofPattern(timeFormat.getValue()));
            case TIMESTAMP:
                return LocalDateTime.parse(
                        (String) field, DateTimeFormatter.ofPattern(datetimeFormat.getValue()));
            case NULL:
                return "";
            case BYTES:
                return field.toString().getBytes(StandardCharsets.UTF_8);
            case ROW:
                String delimiter =
                        ReadonlyConfig.fromConfig(pluginConfig)
                                .get(BaseSourceConfig.FIELD_DELIMITER);
                String[] context = field.toString().split(delimiter);
                SeaTunnelRowType ft = (SeaTunnelRowType) fieldType;
                int length = context.length;
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(length);
                for (int j = 0; j < length; j++) {
                    seaTunnelRow.setField(j, convert(context[j], ft.getFieldType(j)));
                }
                return seaTunnelRow;
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "User defined schema validation failed");
        }
    }

    private <T> boolean isNullOrEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }
}

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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** The XmlReadStrategy class is used to read data from XML files in SeaTunnel. */
@Slf4j
public class XmlReadStrategy extends AbstractReadStrategy {

    private String tableRowName;
    private Boolean useAttrFormat;
    private String delimiter;

    private int fieldCount;

    private DateUtils.Formatter dateFormat;
    private DateTimeUtils.Formatter datetimeFormat;
    private TimeUtils.Formatter timeFormat;
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        preCheckAndInitializeConfiguration();
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        resolveArchiveCompressedInputStream(path, tableId, output, partitionsMap, FileFormat.XML);
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
        SAXReader saxReader = new SAXReader();
        Document document;
        try {
            document = saxReader.read(new InputStreamReader(inputStream, encoding));
        } catch (DocumentException e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, "Failed to read xml file: " + path, e);
        }
        Element rootElement = document.getRootElement();

        fieldCount =
                isMergePartition
                        ? seaTunnelRowTypeWithPartition.getTotalFields()
                        : seaTunnelRowType.getTotalFields();

        rootElement
                .selectNodes(getXPathExpression(tableRowName))
                .forEach(
                        node -> {
                            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fieldCount);

                            List<? extends Node> fields =
                                    new ArrayList<>(
                                                    (useAttrFormat
                                                            ? ((Element) node).attributes()
                                                            : node.selectNodes("./*")))
                                            .stream()
                                                    .filter(
                                                            field ->
                                                                    ArrayUtils.contains(
                                                                            seaTunnelRowType
                                                                                    .getFieldNames(),
                                                                            field.getName()))
                                                    .collect(Collectors.toList());

                            if (CollectionUtils.isEmpty(fields)) return;

                            fields.forEach(
                                    field -> {
                                        int fieldIndex =
                                                ArrayUtils.indexOf(
                                                        seaTunnelRowType.getFieldNames(),
                                                        field.getName());
                                        seaTunnelRow.setField(
                                                fieldIndex,
                                                convert(
                                                        field.getText(),
                                                        seaTunnelRowType
                                                                .getFieldTypes()[fieldIndex]));
                                    });

                            if (isMergePartition) {
                                int partitionIndex = seaTunnelRowType.getTotalFields();
                                for (String value : partitionsMap.values()) {
                                    seaTunnelRow.setField(partitionIndex++, value);
                                }
                            }

                            seaTunnelRow.setTableId(tableId);
                            output.collect(seaTunnelRow);
                        });
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for xml file type");
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        if (ArrayUtils.isEmpty(seaTunnelRowType.getFieldNames())
                || ArrayUtils.isEmpty(seaTunnelRowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Schema information is undefined or misconfigured, please check your configuration file.");
        }

        if (readColumns.isEmpty()) {
            this.seaTunnelRowType = seaTunnelRowType;
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), seaTunnelRowType);
        } else {
            if (readColumns.retainAll(Arrays.asList(seaTunnelRowType.getFieldNames()))) {
                log.warn(
                        "The read columns configuration will be filtered by the schema configuration, this may cause the actual results to be inconsistent with expectations. This is due to read columns not being a subset of the schema, "
                                + "maybe you should check the schema and read_columns!");
            }
            int[] indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
            for (int i = 0; i < readColumns.size(); i++) {
                indexes[i] = seaTunnelRowType.indexOf(readColumns.get(i));
                fields[i] = seaTunnelRowType.getFieldName(indexes[i]);
                types[i] = seaTunnelRowType.getFieldType(indexes[i]);
            }
            this.seaTunnelRowType = new SeaTunnelRowType(fields, types);
            this.seaTunnelRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.seaTunnelRowType);
        }
    }

    @SneakyThrows
    private Object convert(String fieldValue, SeaTunnelDataType<?> fieldType) {
        if (StringUtils.isBlank(fieldValue)) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case STRING:
                return fieldValue;
            case DATE:
                return DateUtils.parse(fieldValue, dateFormat);
            case TIME:
                return TimeUtils.parse(fieldValue, timeFormat);
            case TIMESTAMP:
                return DateTimeUtils.parse(fieldValue, datetimeFormat);
            case TINYINT:
                return (byte) Double.parseDouble(fieldValue);
            case SMALLINT:
                return (short) Double.parseDouble(fieldValue);
            case INT:
                return (int) Double.parseDouble(fieldValue);
            case BIGINT:
                return new BigDecimal(fieldValue).longValue();
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case FLOAT:
                return (float) Double.parseDouble(fieldValue);
            case DECIMAL:
                return new BigDecimal(fieldValue);
            case BOOLEAN:
                return Boolean.parseBoolean(fieldValue);
            case BYTES:
                return fieldValue.getBytes(StandardCharsets.UTF_8);
            case NULL:
                return "";
            case ROW:
                String[] context = fieldValue.split(delimiter);
                SeaTunnelRowType ft = (SeaTunnelRowType) fieldType;
                SeaTunnelRow row = new SeaTunnelRow(context.length);
                IntStream.range(0, context.length)
                        .forEach(i -> row.setField(i, convert(context[i], ft.getFieldTypes()[i])));
                return row;
            case MAP:
            case ARRAY:
                return objectMapper.readValue(fieldValue, fieldType.getTypeClass());
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format("Unsupported data type: %s", sqlType));
        }
    }

    private String getXPathExpression(String tableRowIdentification) {
        return String.format("//%s", tableRowIdentification);
    }

    /** Performs pre-checks and initialization of the configuration for reading XML files. */
    private void preCheckAndInitializeConfiguration() {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        this.tableRowName = readonlyConfig.get(BaseSourceConfigOptions.XML_ROW_TAG);
        this.useAttrFormat = readonlyConfig.get(BaseSourceConfigOptions.XML_USE_ATTR_FORMAT);

        // Check mandatory configurations
        if (StringUtils.isEmpty(tableRowName) || useAttrFormat == null) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Mandatory configurations '%s' and '%s' must be specified when reading XML files.",
                            BaseSourceConfigOptions.XML_ROW_TAG.key(),
                            BaseSourceConfigOptions.XML_USE_ATTR_FORMAT.key()));
        }

        this.delimiter = readonlyConfig.get(BaseSourceConfigOptions.FIELD_DELIMITER);

        this.dateFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.DATE_FORMAT, DateUtils.Formatter::parse);
        this.timeFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.TIME_FORMAT, TimeUtils.Formatter::parse);
        this.datetimeFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.DATETIME_FORMAT, DateTimeUtils.Formatter::parse);
        this.encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
    }

    /**
     * Retrieves the complex date configuration value for the given option.
     *
     * @param option The configuration option to retrieve.
     * @param parser The function used to parse the configuration value.
     * @param <T> The type of the configuration value.
     * @return The parsed configuration value or the default value if not found.
     */
    @SuppressWarnings("unchecked")
    private <T> T getComplexDateConfigValue(Option<?> option, Function<String, T> parser) {
        if (!pluginConfig.hasPath(option.key())) {
            return (T) option.defaultValue();
        }
        return parser.apply(pluginConfig.getString(option.key()));
    }
}

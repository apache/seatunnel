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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink.relational;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.IoTDBv2RelationalRecord;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.RelationalSeaTunnelRowSerializer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import shaded.org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class IoTDBv2RelationalSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SeaTunnelRowSerializer<IoTDBv2RelationalRecord> serializer;
    private final IoTDBv2RelationalSinkClient sinkClient;

    public IoTDBv2RelationalSinkWriter(
            ReadonlyConfig pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        SinkConfig sinkConfig = SinkConfig.loadConfig(pluginConfig);
        List<String> tagKeys = sinkConfig.getKeyTagFields();
        if (tagKeys == null) {
            tagKeys = new ArrayList<>();
        }
        List<String> attributeKeys = sinkConfig.getKeyAttributeFields();
        if (attributeKeys == null) {
            attributeKeys = new ArrayList<>();
        }
        String tableNameKey = sinkConfig.getKeyDevice();
        if (tableNameKey == null) {
            tableNameKey = "";
        }
        String timestampKey = sinkConfig.getKeyTimestamp();
        if (timestampKey == null) {
            timestampKey = "";
        }
        List<String> fieldKeys = sinkConfig.getKeyMeasurementFields();
        List<String> fieldNames =
                createFieldList(
                        seaTunnelRowType,
                        fieldKeys,
                        tagKeys,
                        attributeKeys,
                        tableNameKey,
                        timestampKey);
        List<TSDataType> fieldTypes = createFieldTypeList(seaTunnelRowType, fieldNames);
        this.serializer =
                new RelationalSeaTunnelRowSerializer(
                        seaTunnelRowType,
                        sinkConfig.getStorageGroup(),
                        tableNameKey,
                        timestampKey,
                        tagKeys,
                        attributeKeys,
                        fieldNames,
                        fieldTypes);
        this.sinkClient =
                new IoTDBv2RelationalSinkClient(
                        sinkConfig, tagKeys, attributeKeys, fieldNames, fieldTypes);
    }

    private List<String> createFieldList(
            SeaTunnelRowType seaTunnelRowType,
            List<String> fieldKeys,
            List<String> tagList,
            List<String> attributeList,
            String tableNameKey,
            String timestampKey) {
        if (fieldKeys == null || fieldKeys.isEmpty()) {
            return Stream.of(seaTunnelRowType.getFieldNames())
                    .filter(name -> !tagList.contains(name))
                    .filter(name -> !attributeList.contains(name))
                    .filter(name -> !tableNameKey.equals(name))
                    .filter(name -> !timestampKey.equals(name))
                    .collect(Collectors.toList());
        }
        return fieldKeys;
    }

    private List<TSDataType> createFieldTypeList(
            SeaTunnelRowType seaTunnelRowType, List<String> fieldList) {
        return fieldList.stream()
                .map(
                        field -> {
                            int index = seaTunnelRowType.indexOf(field);
                            SeaTunnelDataType<?> seaTunnelType =
                                    seaTunnelRowType.getFieldType(index);
                            return convert(seaTunnelType);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        IoTDBv2RelationalRecord record = serializer.serialize(element);
        sinkClient.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        sinkClient.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        sinkClient.close();
    }

    private static TSDataType convert(SeaTunnelDataType dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return TSDataType.BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INT:
                return TSDataType.INT32;
            case BIGINT:
                return TSDataType.INT64;
            case FLOAT:
                return TSDataType.FLOAT;
            case DOUBLE:
                return TSDataType.DOUBLE;
            case STRING:
                return TSDataType.STRING;
            case TIMESTAMP:
                return TSDataType.TIMESTAMP;
            case DATE:
                return TSDataType.DATE;
            default:
                throw new IotdbConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + dataType);
        }
    }
}

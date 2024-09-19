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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieKeyException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.convertToSchema;
import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.RowDataToAvroConverters.createConverter;

public class HudiRecordConverter implements Serializable {

    private static final String DEFAULT_PARTITION_PATH = "default";

    private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

    private static final String NULL_RECORD_KEY_PLACEHOLDER = "__null__";

    private static final String EMPTY_RECORD_KEY_PLACEHOLDER = "__empty__";

    public HoodieRecord<HoodieAvroPayload> convertRow(
            Schema schema,
            SeaTunnelRowType seaTunnelRowType,
            SeaTunnelRow element,
            HudiTableConfig hudiTableConfig) {
        GenericRecord rec = new GenericData.Record(schema);
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            rec.put(
                    seaTunnelRowType.getFieldNames()[i],
                    createConverter(seaTunnelRowType.getFieldType(i))
                            .convert(
                                    convertToSchema(seaTunnelRowType.getFieldType(i)),
                                    element.getField(i)));
        }
        return new HoodieAvroRecord<>(
                getHoodieKey(element, seaTunnelRowType, hudiTableConfig),
                new HoodieAvroPayload(Option.of(rec)));
    }

    public HoodieKey getHoodieKey(
            SeaTunnelRow element,
            SeaTunnelRowType seaTunnelRowType,
            HudiTableConfig hudiTableConfig) {
        String partitionPath =
                hudiTableConfig.getPartitionFields() == null
                        ? ""
                        : getRecordPartitionPath(element, seaTunnelRowType, hudiTableConfig);
        String rowKey =
                hudiTableConfig.getRecordKeyFields() == null
                                && hudiTableConfig.getOpType().equals(WriteOperationType.INSERT)
                        ? UUID.randomUUID().toString()
                        : getRecordKey(element, seaTunnelRowType, hudiTableConfig);
        return new HoodieKey(rowKey, partitionPath);
    }

    public String getRecordKey(
            SeaTunnelRow element,
            SeaTunnelRowType seaTunnelRowType,
            HudiTableConfig hudiTableConfig) {
        boolean keyIsNullEmpty = true;
        StringBuilder recordKey = new StringBuilder();
        for (String recordKeyField : hudiTableConfig.getRecordKeyFields().split(",")) {
            String recordKeyValue =
                    getNestedFieldValAsString(element, seaTunnelRowType, recordKeyField);
            recordKeyField = recordKeyField.toLowerCase();
            if (recordKeyValue == null) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(NULL_RECORD_KEY_PLACEHOLDER)
                        .append(",");
            } else if (recordKeyValue.isEmpty()) {
                recordKey
                        .append(recordKeyField)
                        .append(":")
                        .append(EMPTY_RECORD_KEY_PLACEHOLDER)
                        .append(",");
            } else {
                recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
                keyIsNullEmpty = false;
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        if (keyIsNullEmpty) {
            throw new HoodieKeyException(
                    "recordKey values: \""
                            + recordKey
                            + "\" for fields: "
                            + hudiTableConfig.getRecordKeyFields()
                            + " cannot be entirely null or empty.");
        }
        return recordKey.toString();
    }

    public String getRecordPartitionPath(
            SeaTunnelRow element,
            SeaTunnelRowType seaTunnelRowType,
            HudiTableConfig hudiTableConfig) {
        if (hudiTableConfig.getPartitionFields().isEmpty()) {
            return "";
        }

        StringBuilder partitionPath = new StringBuilder();
        String[] avroPartitionPathFields = hudiTableConfig.getPartitionFields().split(",");
        for (String partitionPathField : avroPartitionPathFields) {
            String fieldVal =
                    getNestedFieldValAsString(element, seaTunnelRowType, partitionPathField);
            if (fieldVal == null || fieldVal.isEmpty()) {
                partitionPath.append(partitionPathField).append("=").append(DEFAULT_PARTITION_PATH);
            } else {
                partitionPath.append(partitionPathField).append("=").append(fieldVal);
            }
            partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
        }
        partitionPath.deleteCharAt(partitionPath.length() - 1);
        return partitionPath.toString();
    }

    public String getNestedFieldValAsString(
            SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType, String fieldName) {
        Object value = null;

        if (Arrays.stream(seaTunnelRowType.getFieldNames())
                .collect(Collectors.toList())
                .contains(fieldName)) {
            value = element.getField(seaTunnelRowType.indexOf(fieldName));
        }
        return StringUtils.objToString(value);
    }
}

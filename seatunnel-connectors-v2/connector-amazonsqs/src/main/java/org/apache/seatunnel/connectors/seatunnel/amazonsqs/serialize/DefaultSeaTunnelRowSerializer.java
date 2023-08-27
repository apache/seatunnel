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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {

    private final SeaTunnelRowType seaTunnelRowType;
    private final AmazonSqsSourceOptions amazonSqsSourceOptions;
    private final List<MessageAttributeValue> measurementsType;

    public DefaultSeaTunnelRowSerializer(
            SeaTunnelRowType seaTunnelRowType, AmazonSqsSourceOptions AmazonSqsSourceOptions) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.amazonSqsSourceOptions = AmazonSqsSourceOptions;
        this.measurementsType = convertTypes(seaTunnelRowType);
    }

    @Override
    public SendMessageRequest serialize(SeaTunnelRow seaTunnelRow) {
        String messageBody = serializeRowToString(seaTunnelRow);

        return SendMessageRequest.builder()
                .queueUrl(amazonSqsSourceOptions.getUrl())
                .messageBody(messageBody)
                .build();
    }

    public String serializeRowToString(SeaTunnelRow seaTunnelRow) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int index = 0; index < seaTunnelRowType.getFieldNames().length; index++) {
            Object fieldValue = seaTunnelRow.getField(index);
            stringBuilder
                    .append(seaTunnelRowType.getFieldName(index))
                    .append(": ")
                    .append(fieldValue)
                    .append(System.lineSeparator());
        }
        return stringBuilder.toString();
    }

    private List<MessageAttributeValue> convertTypes(SeaTunnelRowType seaTunnelRowType) {
        return Arrays.stream(seaTunnelRowType.getFieldTypes())
                .map(this::convertType)
                .collect(Collectors.toList());
    }

    private MessageAttributeValue convertType(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return MessageAttributeValue.builder()
                        .dataType("Number")
                        .stringValue("0") // Change this value based on your use case
                        .build();
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("AttributeValue") // Change this value based on your use case
                        .build();
            case BOOLEAN:
                return MessageAttributeValue.builder()
                        .dataType("String") // Boolean as a string
                        .stringValue(
                                Boolean.toString(false)) // Change this value based on your use case
                        .build();
            case NULL:
                return MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("NULL")
                        .build();
            case BYTES:
                return MessageAttributeValue.builder()
                        .dataType("Binary")
                        .binaryValue(SdkBytes.fromByteArray(new byte[0]))
                        .build();
            case MAP:
            case ARRAY:
            default:
                throw new AmazonSqsConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + seaTunnelDataType);
        }
    }

    private MessageAttributeValue convertItem(
            Object value,
            SeaTunnelDataType seaTunnelDataType,
            MessageAttributeValue measurementsType) {
        if (value == null) {
            return MessageAttributeValue.builder().dataType("String").stringValue("NULL").build();
        }
        switch (measurementsType.dataType()) {
            case "Number":
                return MessageAttributeValue.builder()
                        .dataType("Number")
                        .stringValue(value.toString()) // Convert value to string for numeric types
                        .build();
            case "String":
                return MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue((String) value)
                        .build();
            case "Binary":
                return MessageAttributeValue.builder()
                        .dataType("Binary")
                        .binaryValue(SdkBytes.fromByteArray((byte[]) value))
                        .build();
            case "Boolean":
                return MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(Boolean.toString((Boolean) value))
                        .build();
            case "Null":
                return MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("NULL")
                        .build();
            case "StringSet":
            case "NumberSet":
            case "BinarySet":
            case "Map":
            case "List":
            default:
                throw new AmazonSqsConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + measurementsType.dataType());
        }
    }
}

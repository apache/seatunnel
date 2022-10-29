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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazondynamodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AmazondynamodbWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final AmazondynamodbSourceOptions amazondynamodbSourceOptions;
    private final SeaTunnelRowType seaTunnelRowType;
    private final DynamoDbClient dynamoDbClient;
    private final List<AttributeValue.Type> measurementsType;

    public AmazondynamodbWriter(AmazondynamodbSourceOptions amazondynamodbSourceOptions, SeaTunnelRowType seaTunnelRowType) {
        this.amazondynamodbSourceOptions = amazondynamodbSourceOptions;
        this.seaTunnelRowType = seaTunnelRowType;
        dynamoDbClient = DynamoDbClient.builder()
            .endpointOverride(URI.create(amazondynamodbSourceOptions.getUrl()))
            // The region is meaningless for local DynamoDb but required for client builder validation
            .region(Region.of(amazondynamodbSourceOptions.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(amazondynamodbSourceOptions.getAccessKeyId(), amazondynamodbSourceOptions.getSecretAccessKey())))
            .build();
        this.measurementsType = convertTypes(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        dynamoDbClient.putItem(convertRow(element, seaTunnelRowType));
    }

    @Override
    public void close() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }

    private PutItemRequest convertRow(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();
        for (int index = 0; index < seaTunnelRowType.getFieldNames().length; index++) {
            itemValues.put(seaTunnelRowType.getFieldName(index), convertItem(element.getField(index),
                seaTunnelRowType.getFieldType(index),
                measurementsType.get(index)));
        }
        return PutItemRequest.builder()
            .tableName(amazondynamodbSourceOptions.getTable())
            .item(itemValues)
            .build();
    }

    private AttributeValue convertItem(Object value, SeaTunnelDataType seaTunnelDataType, AttributeValue.Type measurementsType) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }
        switch (measurementsType) {
            case N:
                return AttributeValue.builder().n(Integer.toString(((Number) value).intValue())).build();
            case S:
                return AttributeValue.builder().s(String.valueOf(value)).build();
            case BOOL:
                return AttributeValue.builder().bool((Boolean) value).build();
            case B:
                return AttributeValue.builder().b(SdkBytes.fromByteArrayUnsafe((byte[]) value)).build();
            case SS:
                return AttributeValue.builder().ss((Collection<String>) value).build();
            case NS:
                return AttributeValue.builder().ns(((Collection<Number>) value)
                    .stream().map(Object::toString).collect(Collectors.toList())).build();
            case BS:
                return AttributeValue.builder().bs(
                    ((Collection<Number>) value)
                        .stream().map(number ->
                            SdkBytes.fromByteArray((byte[]) value)).collect(Collectors.toList())
                ).build();
            case M:
                MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelDataType;
                Map<String, Object> map = (Map) value;
                Map<String, AttributeValue> resultMap = new HashMap<>(map.size());
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String mapKeyName = entry.getKey();
                    resultMap.put(mapKeyName, convertItem(entry.getValue(), mapType.getValueType(), convertType(mapType.getValueType())));
                }
                return AttributeValue.builder().m(resultMap).build();
            case L:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelDataType;
                BasicType<?> elementType = arrayType.getElementType();
                return AttributeValue.builder().l(convertItem(value, elementType, convertType(elementType))).build();
            case NUL:
                return AttributeValue.builder().nul(true).build();
            default:
                throw new UnsupportedOperationException("Unsupported dataType: " + measurementsType);
        }
    }

    private List<AttributeValue.Type> convertTypes(SeaTunnelRowType seaTunnelRowType) {
        return Arrays.stream(seaTunnelRowType.getFieldTypes()).map(seaTunnelDataType -> convertType(seaTunnelDataType)).collect(Collectors.toList());
    }

    private AttributeValue.Type convertType(SeaTunnelDataType seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case INT:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return AttributeValue.Type.N;
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return AttributeValue.Type.S;
            case BOOLEAN:
                return AttributeValue.Type.BOOL;
            case NULL:
                return AttributeValue.Type.NUL;
            case BYTES:
                return AttributeValue.Type.B;
            case MAP:
                return AttributeValue.Type.M;
            case ARRAY:
                return AttributeValue.Type.L;
            default:
                throw new UnsupportedOperationException("Unsupported dataType: " + seaTunnelDataType);
        }
    }
}

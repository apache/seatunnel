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

package org.apache.seatunnel.connectors.seatunnel.milvus.convert;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Lists;

import com.google.protobuf.ProtocolStringList;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.utils.JacksonUtils;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.grpc.ShowType;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.index.DescribeIndexParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MilvusConvertUtils {

    private static final String CATALOG_NAME = "Milvus";

    public static Map<TablePath, CatalogTable> getSourceTables(ReadonlyConfig config) {
        MilvusServiceClient client =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(config.get(MilvusSourceConfig.URL))
                                .withToken(config.get(MilvusSourceConfig.TOKEN))
                                .build());

        String database = config.get(MilvusSourceConfig.DATABASE);
        List<String> collectionList = new ArrayList<>();
        if (StringUtils.isNotEmpty(config.get(MilvusSourceConfig.COLLECTION))) {
            collectionList.add(config.get(MilvusSourceConfig.COLLECTION));
        } else {
            R<ShowCollectionsResponse> response =
                    client.showCollections(
                            ShowCollectionsParam.newBuilder()
                                    .withDatabaseName(database)
                                    .withShowType(ShowType.All)
                                    .build());
            if (response.getStatus() != R.Status.Success.getCode()) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.SHOW_COLLECTIONS_ERROR);
            }

            ProtocolStringList collections = response.getData().getCollectionNamesList();
            if (CollectionUtils.isEmpty(collections)) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.DATABASE_NO_COLLECTIONS, database);
            }
            collectionList.addAll(collections);
        }

        Map<TablePath, CatalogTable> map = new HashMap<>();
        for (String collection : collectionList) {
            CatalogTable catalogTable = getCatalogTable(client, database, collection);
            map.put(TablePath.of(database, collection), catalogTable);
        }
        return map;
    }

    public static CatalogTable getCatalogTable(
            MilvusServiceClient client, String database, String collection) {
        TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, database, collection);
        TableSchema tableSchema = getTableSchema(client, database, collection);

        CatalogTable catalogTable =
                CatalogTable.of(tableId, tableSchema, new HashMap<>(), new ArrayList<>(), null);
        return catalogTable;
    }

    public static TableSchema getTableSchema(
            MilvusServiceClient client, String database, String collection) {
        R<DescribeCollectionResponse> response =
                client.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());

        if (response.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.DESC_COLLECTION_ERROR);
        }

        // collection column
        DescribeCollectionResponse data = response.getData();
        CollectionSchema schema = data.getSchema();
        List<Column> columns = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            columns.add(MilvusConvertUtils.convertColumn(fieldSchema));
        }

        // primary key
        PrimaryKey primaryKey = buildPrimaryKey(schema.getFieldsList());

        // index
        R<DescribeIndexResponse> describeIndexResponseR =
                client.describeIndex(
                        DescribeIndexParam.newBuilder()
                                .withDatabaseName(database)
                                .withCollectionName(collection)
                                .build());
        if (describeIndexResponseR.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.DESC_INDEX_ERROR);
        }
        DescribeIndexResponse indexResponse = describeIndexResponseR.getData();
        List<VectorIndex> vectorIndexes = buildVectorIndexes(indexResponse);

        return TableSchema.builder()
                .columns(columns)
                .primaryKey(primaryKey)
                .enableDynamicField(schema.getEnableDynamicField())
                .vectorIndexes(vectorIndexes)
                .build();
    }

    private static List<VectorIndex> buildVectorIndexes(DescribeIndexResponse indexResponse) {
        if (CollectionUtils.isEmpty(indexResponse.getIndexDescriptionsList())) {
            return null;
        }

        List<VectorIndex> list = new ArrayList<>();
        for (IndexDescription per : indexResponse.getIndexDescriptionsList()) {
            Map<String, String> paramsMap =
                    per.getParamsList().stream()
                            .collect(
                                    Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue));

            VectorIndex index =
                    VectorIndex.builder()
                            .fieldName(per.getFieldName())
                            .indexName(per.getIndexName())
                            .metricType(paramsMap.get("metric_type"))
                            .indexType(paramsMap.get("index_type"))
                            .build();

            list.add(index);
        }

        return list;
    }

    public static PrimaryKey buildPrimaryKey(List<FieldSchema> fields) {
        for (FieldSchema field : fields) {
            if (field.getIsPrimaryKey()) {
                return PrimaryKey.of(
                        field.getName(), Lists.newArrayList(field.getName()), field.getAutoID());
            }
        }

        return null;
    }

    public static PhysicalColumn convertColumn(FieldSchema fieldSchema) {
        DataType dataType = fieldSchema.getDataType();
        PhysicalColumn.PhysicalColumnBuilder builder = PhysicalColumn.builder();
        builder.name(fieldSchema.getName());
        builder.sourceType(dataType.name());
        builder.comment(fieldSchema.getDescription());

        switch (dataType) {
            case Bool:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case Int8:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case Int16:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case Int32:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case Int64:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case Float:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case Double:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case VarChar:
                builder.dataType(BasicType.STRING_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("max_length")) {
                        builder.columnLength(Long.parseLong(keyValuePair.getValue()) * 4);
                        break;
                    }
                }
                break;
            case String:
            case JSON:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case Array:
                builder.dataType(ArrayType.STRING_ARRAY_TYPE);
                break;
            case FloatVector:
                builder.dataType(VectorType.VECTOR_FLOAT_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case BinaryVector:
                builder.dataType(VectorType.VECTOR_BINARY_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case SparseFloatVector:
                builder.dataType(VectorType.VECTOR_SPARSE_FLOAT_TYPE);
                break;
            case Float16Vector:
                builder.dataType(VectorType.VECTOR_FLOAT16_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            case BFloat16Vector:
                builder.dataType(VectorType.VECTOR_BFLOAT16_TYPE);
                for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
                    if (keyValuePair.getKey().equals("dim")) {
                        builder.scale(Integer.valueOf(keyValuePair.getValue()));
                        break;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType);
        }

        return builder.build();
    }

    public static Object convertBySeaTunnelType(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return Integer.parseInt(value.toString());
            case BIGINT:
                return Long.parseLong(value.toString());
            case SMALLINT:
                return Short.parseShort(value.toString());
            case STRING:
            case DATE:
                return value.toString();
            case FLOAT_VECTOR:
                List<Float> vector = new ArrayList<>();
                for (Object o : (Object[]) value) {
                    vector.add(Float.parseFloat(o.toString()));
                }
                return vector;
            case FLOAT:
                return Float.parseFloat(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                switch (arrayType.getElementType().getSqlType()) {
                    case STRING:
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    case INT:
                        Integer[] intArray = (Integer[]) value;
                        return Arrays.asList(intArray);
                    case BIGINT:
                        Long[] longArray = (Long[]) value;
                        return Arrays.asList(longArray);
                    case FLOAT:
                        Float[] floatArray = (Float[]) value;
                        return Arrays.asList(floatArray);
                    case DOUBLE:
                        Double[] doubleArray = (Double[]) value;
                        return Arrays.asList(doubleArray);
                }
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) value;
                return JsonUtils.toJsonString(row.getFields());
            case MAP:
                return JacksonUtils.toJsonString(value);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }
}

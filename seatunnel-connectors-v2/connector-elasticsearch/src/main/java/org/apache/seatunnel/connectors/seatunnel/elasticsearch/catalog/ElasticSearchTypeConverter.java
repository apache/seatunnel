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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeConverter;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.AGGREGATE_METRIC_DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.BINARY;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.BOOLEAN;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.BYTE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.COMPLETION;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DATE_NANOS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DATE_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DENSE_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DOUBLE_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.FLATTENED;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.FLOAT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.FLOAT_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.GEO_POINT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.GEO_SHAPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.HALF_FLOAT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.HISTOGRAM;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.INTEGER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.INTEGER_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.IP;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.IP_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.JOIN;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.KEYWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.LONG;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.LONG_RANGE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.MATCH_ONLY_TEXT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.NESTED;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.OBJECT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.PERCOLATOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.POINT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.RANK_FEATURE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.RANK_FEATURES;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.SEARCH_AS_YOU_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.SHAPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.SHORT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.SPARSE_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.STRING;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.TEXT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.TOKEN_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.UNSIGNED_LONG;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.VERSION;

@AutoService(TypeConverter.class)
public class ElasticSearchTypeConverter implements BasicTypeConverter<BasicTypeDefine<EsType>> {
    public static final ElasticSearchTypeConverter INSTANCE = new ElasticSearchTypeConverter();

    @Override
    public String identifier() {
        return "Elasticsearch";
    }

    @Override
    public Column convert(BasicTypeDefine<EsType> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String type = typeDefine.getDataType().toLowerCase();
        switch (type) {
            case AGGREGATE_METRIC_DOUBLE:
                List<String> metrics =
                        (List<String>) typeDefine.getNativeType().getOptions().get("metrics");
                builder.dataType(
                        new SeaTunnelRowType(
                                metrics.toArray(new String[0]),
                                metrics.stream()
                                        .map(s -> BasicType.DOUBLE_TYPE)
                                        .toArray(SeaTunnelDataType<?>[]::new)));
                break;
            case DENSE_VECTOR:
                String elementType =
                        typeDefine.getNativeType().getOptions().get("element_type").toString();
                if (elementType.equals("byte")) {
                    builder.dataType(ArrayType.BYTE_ARRAY_TYPE);
                } else {
                    builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
                }
                break;
            case BYTE:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(3);
                break;
            case DATE_NANOS:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(9);
                break;
            case DOUBLE:
            case RANK_FEATURE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case FLOAT:
            case HALF_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case HISTOGRAM:
                SeaTunnelRowType rowType =
                        new SeaTunnelRowType(
                                new String[] {"values", "counts"},
                                new SeaTunnelDataType<?>[] {
                                    ArrayType.DOUBLE_ARRAY_TYPE, ArrayType.LONG_ARRAY_TYPE
                                });
                builder.dataType(rowType);
                break;
            case INTEGER:
            case TOKEN_COUNT:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case LONG:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case SHORT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case OBJECT:
                Map<String, BasicTypeDefine<EsType>> typeInfo =
                        (Map) typeDefine.getNativeType().getOptions();
                SeaTunnelRowType object =
                        new SeaTunnelRowType(
                                typeInfo.keySet().toArray(new String[0]),
                                typeInfo.values().stream()
                                        .map(this::convert)
                                        .map(Column::getDataType)
                                        .toArray(SeaTunnelDataType<?>[]::new));
                builder.dataType(object);
                break;
            case INTEGER_RANGE:
                builder.dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.INT_TYPE));
                break;
            case FLOAT_RANGE:
                builder.dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.FLOAT_TYPE));
                break;
            case LONG_RANGE:
                builder.dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.LONG_TYPE));
                break;
            case DOUBLE_RANGE:
                builder.dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.DOUBLE_TYPE));
                break;
            case DATE_RANGE:
                builder.dataType(
                        new MapType<>(BasicType.STRING_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE));
                break;
            case IP_RANGE:
                builder.dataType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
                break;
            case UNSIGNED_LONG:
                builder.dataType(new DecimalType(20, 0));
                builder.columnLength(20L);
                builder.scale(0);
                break;
            case TEXT:
            case BINARY:
            case VERSION:
            case IP:
            case JOIN:
            case KEYWORD:
            case FLATTENED:
            case GEO_POINT:
            case COMPLETION:
            case STRING:
            case GEO_SHAPE:
            case NESTED:
            case PERCOLATOR:
            case POINT:
            case RANK_FEATURES:
            case SEARCH_AS_YOU_TYPE:
            case SPARSE_VECTOR:
            case MATCH_ONLY_TEXT:
            case SHAPE:
            default:
                builder.dataType(BasicType.STRING_TYPE);
                break;
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<EsType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<EsType> builder =
                BasicTypeDefine.<EsType>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(BOOLEAN);
                builder.dataType(BOOLEAN);
                builder.nativeType(new EsType(BOOLEAN, new HashMap<>()));
                break;
            case BYTES:
                builder.columnType(BINARY);
                builder.dataType(BINARY);
                builder.nativeType(new EsType(BINARY, new HashMap<>()));
                break;
            case TINYINT:
                builder.columnType(BYTE);
                builder.dataType(BYTE);
                builder.nativeType(new EsType(BYTE, new HashMap<>()));
                break;
            case SMALLINT:
                builder.columnType(SHORT);
                builder.dataType(SHORT);
                builder.nativeType(new EsType(SHORT, new HashMap<>()));
                break;
            case INT:
                builder.columnType(INTEGER);
                builder.dataType(INTEGER);
                builder.nativeType(new EsType(INTEGER, new HashMap<>()));
                break;
            case BIGINT:
                builder.columnType(LONG);
                builder.dataType(LONG);
                builder.nativeType(new EsType(LONG, new HashMap<>()));
                break;
            case FLOAT:
                builder.columnType(FLOAT);
                builder.dataType(FLOAT);
                builder.nativeType(new EsType(FLOAT, new HashMap<>()));
                break;
            case DOUBLE:
                builder.columnType(DOUBLE);
                builder.dataType(DOUBLE);
                builder.nativeType(new EsType(DOUBLE, new HashMap<>()));
                break;
            case DATE:
            case TIMESTAMP:
                Map<String, Object> option = new HashMap<>();
                if (column.getScale() != null && column.getScale() > 3) {
                    option.put("format", "strict_date_optional_time||epoch_millis");
                    builder.columnType(DATE_NANOS);
                    builder.dataType(DATE_NANOS);
                    builder.nativeType(new EsType(DATE_NANOS, option));
                } else {
                    option.put("format", "strict_date_optional_time_nanos||epoch_millis");
                    builder.columnType(DATE);
                    builder.dataType(DATE);
                    builder.nativeType(new EsType(DATE, option));
                }
                break;
            case DECIMAL:
                builder.columnType(TEXT);
                builder.dataType(TEXT);
                builder.nativeType(new EsType(TEXT, new HashMap<>()));
                break;
            case MAP:
                builder.columnType(FLATTENED);
                builder.dataType(FLATTENED);
                builder.nativeType(new EsType(FLATTENED, new HashMap<>()));
                break;
            case ARRAY:
                SeaTunnelDataType type = ((ArrayType) column.getDataType()).getElementType();
                if (type.equals(BasicType.BYTE_TYPE)) {
                    builder.columnType(BINARY);
                    builder.dataType(BINARY);
                    builder.nativeType(new EsType(BINARY, new HashMap<>()));
                } else if (type.equals(BasicType.SHORT_TYPE)) {
                    builder.columnType(SHORT);
                    builder.dataType(SHORT);
                    builder.nativeType(new EsType(SHORT, new HashMap<>()));
                } else if (type.equals(BasicType.INT_TYPE)) {
                    builder.columnType(INTEGER);
                    builder.dataType(INTEGER);
                    builder.nativeType(new EsType(INTEGER, new HashMap<>()));
                } else if (type.equals(BasicType.LONG_TYPE)) {
                    builder.columnType(LONG);
                    builder.dataType(LONG);
                    builder.nativeType(new EsType(LONG, new HashMap<>()));
                } else if (type.equals(BasicType.FLOAT_TYPE)) {
                    builder.columnType(FLOAT);
                    builder.dataType(FLOAT);
                    builder.nativeType(new EsType(FLOAT, new HashMap<>()));
                } else if (type.equals(BasicType.DOUBLE_TYPE)) {
                    builder.columnType(DOUBLE);
                    builder.dataType(DOUBLE);
                    builder.nativeType(new EsType(DOUBLE, new HashMap<>()));
                } else if (type.equals(BasicType.STRING_TYPE)) {
                    builder.columnType(TEXT);
                    builder.dataType(TEXT);
                    builder.nativeType(new EsType(TEXT, new HashMap<>()));
                } else {
                    builder.columnType(TEXT);
                    builder.dataType(TEXT);
                    builder.nativeType(new EsType(TEXT, new HashMap<>()));
                }
                break;
            case ROW:
                builder.columnType(OBJECT);
                builder.dataType(OBJECT);
                SeaTunnelRowType row = (SeaTunnelRowType) column.getDataType();
                Map<String, BasicTypeDefine<EsType>> typeInfo = new HashMap<>();
                for (int i = 0; i < row.getFieldNames().length; i++) {
                    typeInfo.put(
                            row.getFieldName(i),
                            reconvert(
                                    PhysicalColumn.of(
                                            row.getFieldName(i),
                                            row.getFieldType(i),
                                            (Long) null,
                                            true,
                                            null,
                                            null)));
                }
                builder.nativeType(new EsType(OBJECT, (Map) typeInfo));
                break;
            case TIME:
            case NULL:
            case STRING:
            default:
                builder.columnType(TEXT);
                builder.dataType(TEXT);
                builder.nativeType(new EsType(TEXT, new HashMap<>()));
        }
        return builder.build();
    }
}

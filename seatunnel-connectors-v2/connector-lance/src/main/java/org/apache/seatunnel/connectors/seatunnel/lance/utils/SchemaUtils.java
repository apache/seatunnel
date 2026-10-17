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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.data.LanceTypeMapper;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.lancedb.lance.namespace.model.JsonArrowDataType;
import com.lancedb.lance.namespace.model.JsonArrowField;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.util.ArrowIpcUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** The util seatunnel schema to lance schema */
public class SchemaUtils {

    public static SeaTunnelDataType<?> toSeaTunnelType(String field, JsonArrowDataType type) {
        return LanceTypeMapper.INSTANCE.convertDataType(field, type);
    }

    public static Schema convertSchema(SeaTunnelRow element, SeaTunnelRowType seaTunnelRowType) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        List<Field> fieldList = Lists.newArrayList();
        for (int i = 0; i < fieldTypes.length; i++) {
            Object fieldValue = element.getField(i);
            if (Objects.nonNull(fieldValue)) {
                String fieldName = seaTunnelRowType.getFieldName(i);
                Field field;
                switch (fieldTypes[i].getSqlType()) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        field = Field.nullable(fieldName, new ArrowType.Int(32, true));
                        fieldList.add(field);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        field =
                                Field.nullable(
                                        fieldName,
                                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
                        fieldList.add(field);
                        break;
                    case STRING:
                        field = Field.nullable(fieldName, new ArrowType.Utf8());
                        fieldList.add(field);
                        break;
                    case BOOLEAN:
                        field = Field.nullable(fieldName, new ArrowType.Bool());
                        fieldList.add(field);
                        break;
                    case NULL:
                        field = Field.nullable(fieldName, new ArrowType.Null());
                        fieldList.add(field);
                        break;
                    case DECIMAL:
                        int precision = 38;
                        int scale = 10;
                        if (fieldTypes[i] instanceof DecimalType) {
                            DecimalType decimalType = (DecimalType) fieldTypes[i];
                            precision = decimalType.getPrecision();
                            scale = decimalType.getScale();
                        }
                        // Arrow Decimal128 supports up to 38 digits precision
                        // Use Decimal128 (bitWidth=128) for better compatibility
                        field =
                                Field.nullable(
                                        fieldName, new ArrowType.Decimal(precision, scale, 128));
                        fieldList.add(field);
                        break;
                    case BYTES:
                        field = Field.nullable(fieldName, new ArrowType.Binary());
                        fieldList.add(field);
                        break;
                    case DATE:
                        field = Field.nullable(fieldName, new ArrowType.Date(DateUnit.DAY));
                        fieldList.add(field);
                        break;
                    case TIME:
                        field =
                                Field.nullable(
                                        fieldName, new ArrowType.Time(TimeUnit.MILLISECOND, 32));
                        fieldList.add(field);
                        break;
                    case TIMESTAMP:
                        field =
                                Field.nullable(
                                        fieldName,
                                        new ArrowType.Timestamp(
                                                TimeUnit.MICROSECOND, "Asia/Shanghai"));
                        fieldList.add(field);
                        break;
                    case MAP:
                        field = toMapField(fieldName, fieldTypes[i], true);
                        fieldList.add(field);
                        break;
                    case ARRAY:
                        field = Field.nullable(fieldName, new ArrowType.List());
                        fieldList.add(field);
                        break;
                    default:
                        throw CommonError.unsupportedDataType(
                                LanceCommonConfig.CONNECTOR_IDENTITY,
                                seaTunnelRowType.getFieldType(i).getSqlType().toString(),
                                fieldName);
                }
            }
        }

        return new Schema(fieldList);
    }

    /**
     * Builds a Map field following Arrow's canonical Map layout: a non-nullable {@code entries}
     * Struct that contains a non-nullable {@code key} and a nullable {@code value}.
     */
    private static Field toMapField(
            String fieldName, SeaTunnelDataType<?> dataType, boolean nullable) {
        if (!(dataType instanceof MapType)) {
            throw CommonError.unsupportedDataType(
                    LanceCommonConfig.CONNECTOR_IDENTITY, "MAP", fieldName);
        }
        MapType<?, ?> mapType = (MapType<?, ?>) dataType;
        Field keyField = toArrowField("key", mapType.getKeyType(), false);
        Field valueField = toArrowField("value", mapType.getValueType(), true);
        Field entriesField =
                new Field(
                        "entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        Arrays.asList(keyField, valueField));
        return new Field(
                fieldName,
                new FieldType(nullable, new ArrowType.Map(false), null),
                Collections.singletonList(entriesField));
    }

    private static Field toArrowField(
            String fieldName, SeaTunnelDataType<?> dataType, boolean nullable) {
        if (dataType.getSqlType() == SqlType.MAP) {
            return toMapField(fieldName, dataType, nullable);
        }
        if (dataType.getSqlType() == SqlType.ARRAY) {
            if (!(dataType instanceof ArrayType)) {
                throw CommonError.unsupportedDataType(
                        LanceCommonConfig.CONNECTOR_IDENTITY,
                        dataType.getSqlType().toString(),
                        fieldName);
            }
            ArrayType<?, ?> arrayType = (ArrayType<?, ?>) dataType;
            Field elementField = toArrowField("element", arrayType.getElementType(), true);
            return new Field(
                    fieldName,
                    new FieldType(nullable, new ArrowType.List(), null),
                    Collections.singletonList(elementField));
        }
        return new Field(
                fieldName,
                new FieldType(nullable, toArrowScalarType(fieldName, dataType), null),
                null);
    }

    private static ArrowType toArrowScalarType(String fieldName, SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new ArrowType.Int(32, true);
            case FLOAT:
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case STRING:
                return new ArrowType.Utf8();
            case BOOLEAN:
                return new ArrowType.Bool();
            case NULL:
                return new ArrowType.Null();
            case DECIMAL:
                int precision = 38;
                int scale = 10;
                if (dataType instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) dataType;
                    precision = decimalType.getPrecision();
                    scale = decimalType.getScale();
                }
                return new ArrowType.Decimal(precision, scale, 128);
            case BYTES:
                return new ArrowType.Binary();
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai");
            default:
                break;
        }
        throw CommonError.unsupportedDataType(
                LanceCommonConfig.CONNECTOR_IDENTITY, dataType.getSqlType().toString(), fieldName);
    }

    public static JsonArrowSchema convertJsonArrowSchema(TableSchema schema) {
        List<JsonArrowField> fields = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            JsonArrowDataType dataType =
                    LanceTypeMapper.INSTANCE.convertJsonArrowType(
                            column.getName(), column.getDataType());
            JsonArrowField field = new JsonArrowField();
            field.setName(column.getName());
            field.setType(dataType);
            field.setNullable(column.isNullable());
            fields.add(field);
        }

        JsonArrowSchema arrowSchema = new JsonArrowSchema();
        arrowSchema.setFields(fields);
        return arrowSchema;
    }

    public static byte[] convertJsonArrowSchemaToBytes(TableSchema schema) throws IOException {
        JsonArrowSchema jsonArrowSchema = convertJsonArrowSchema(schema);
        return ArrowIpcUtil.createEmptyArrowIpcStream(jsonArrowSchema);
    }
}

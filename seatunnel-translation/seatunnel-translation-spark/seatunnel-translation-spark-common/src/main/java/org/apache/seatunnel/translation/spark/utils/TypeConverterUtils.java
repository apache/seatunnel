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

package org.apache.seatunnel.translation.spark.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TypeConverterUtils {
    private static final Map<DataType, SeaTunnelDataType<?>> TO_SEA_TUNNEL_TYPES =
            new HashMap<>(16);
    public static final String ROW_KIND_FIELD = "op";

    static {
        TO_SEA_TUNNEL_TYPES.put(DataTypes.NullType, BasicType.VOID_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.StringType, BasicType.STRING_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BooleanType, BasicType.BOOLEAN_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ByteType, BasicType.BYTE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ShortType, BasicType.SHORT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.IntegerType, BasicType.INT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.LongType, BasicType.LONG_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.FloatType, BasicType.FLOAT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DoubleType, BasicType.DOUBLE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BinaryType, PrimitiveByteArrayType.INSTANCE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DateType, LocalTimeType.LOCAL_DATE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.TimestampType, LocalTimeType.LOCAL_DATE_TIME_TYPE);
    }

    private TypeConverterUtils() {
        throw new UnsupportedOperationException(
                "TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static DataType convert(SeaTunnelDataType<?> dataType) {
        checkNotNull(dataType, "The SeaTunnel's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
                return DataTypes.NullType;
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case TINYINT:
                return DataTypes.ByteType;
            case SMALLINT:
                return DataTypes.ShortType;
            case INT:
                return DataTypes.IntegerType;
            case BIGINT:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case BYTES:
                return DataTypes.BinaryType;
            case DATE:
                return DataTypes.DateType;
                // case TIME:
                // TODO: not support now, how reconvert?
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case ARRAY:
                return DataTypes.createArrayType(
                        convert(((ArrayType<?, ?>) dataType).getElementType()));
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                return DataTypes.createMapType(
                        convert(mapType.getKeyType()), convert(mapType.getValueType()));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return new org.apache.spark.sql.types.DecimalType(
                        decimalType.getPrecision(), decimalType.getScale());
            case ROW:
                return convert((SeaTunnelRowType) dataType);
            default:
        }
        throw new IllegalArgumentException("Unsupported SeaTunnel's data type: " + dataType);
    }

    private static StructType convert(SeaTunnelRowType rowType) {
        // TODO: row kind
        StructField[] fields = new StructField[rowType.getFieldNames().length];
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            fields[i] =
                    new StructField(
                            rowType.getFieldNames()[i],
                            convert(rowType.getFieldTypes()[i]),
                            true,
                            Metadata.empty());
        }
        return new StructType(fields);
    }

    public static SeaTunnelDataType<?> convert(DataType sparkType) {
        checkNotNull(sparkType, "The Spark's data type is required.");
        SeaTunnelDataType<?> dataType = TO_SEA_TUNNEL_TYPES.get(sparkType);
        if (dataType != null) {
            return dataType;
        }
        if (sparkType instanceof org.apache.spark.sql.types.ArrayType) {
            return convert((org.apache.spark.sql.types.ArrayType) sparkType);
        }
        if (sparkType instanceof org.apache.spark.sql.types.MapType) {
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) sparkType;
            return new MapType<>(convert(mapType.keyType()), convert(mapType.valueType()));
        }
        if (sparkType instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType decimalType =
                    (org.apache.spark.sql.types.DecimalType) sparkType;
            return new DecimalType(decimalType.precision(), decimalType.scale());
        }
        if (sparkType instanceof StructType) {
            return convert((StructType) sparkType);
        }
        throw new IllegalArgumentException("Unsupported Spark's data type: " + sparkType.sql());
    }

    private static ArrayType<?, ?> convert(org.apache.spark.sql.types.ArrayType arrayType) {
        switch (convert(arrayType.elementType()).getSqlType()) {
            case STRING:
                return ArrayType.STRING_ARRAY_TYPE;
            case BOOLEAN:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case TINYINT:
                return ArrayType.BYTE_ARRAY_TYPE;
            case SMALLINT:
                return ArrayType.SHORT_ARRAY_TYPE;
            case INT:
                return ArrayType.INT_ARRAY_TYPE;
            case BIGINT:
                return ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported Spark's array type: %s.", arrayType.sql()));
        }
    }

    private static SeaTunnelRowType convert(StructType structType) {
        StructField[] structFields = structType.fields();
        String[] fieldNames = new String[structFields.length];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[structFields.length];
        for (int i = 0; i < structFields.length; i++) {
            fieldNames[i] = structFields[i].name();
            fieldTypes[i] = convert(structFields[i].dataType());
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}

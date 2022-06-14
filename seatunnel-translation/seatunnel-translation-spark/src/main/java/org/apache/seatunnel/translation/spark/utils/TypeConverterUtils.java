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
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.PojoType;
import org.apache.seatunnel.api.table.type.PrimitiveArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.TimestampType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ObjectType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VarcharType;

import java.util.HashMap;
import java.util.Map;

public class TypeConverterUtils {

    private static final Map<SeaTunnelDataType<?>, DataType> SEA_TUNNEL_TO_SPARK_TYPES = new HashMap<>(16);

    static {
        // basic types
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.STRING_TYPE, DataTypes.StringType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.BOOLEAN_TYPE, DataTypes.BooleanType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.BYTE_TYPE, DataTypes.ByteType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.SHORT_TYPE, DataTypes.ShortType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.INT_TYPE, DataTypes.IntegerType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.LONG_TYPE, DataTypes.LongType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.FLOAT_TYPE, DataTypes.FloatType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.DOUBLE_TYPE, DataTypes.DoubleType);
        // todo: need to confirm
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.CHAR_TYPE, new VarcharType(1));
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.BIG_INT_TYPE, DataTypes.LongType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.BIG_DECIMAL_TYPE, new DecimalType());
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.VOID_TYPE, DataTypes.NullType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(BasicType.DATE_TYPE, DataTypes.DateType);
        SEA_TUNNEL_TO_SPARK_TYPES.put(PrimitiveArrayType.PRIMITIVE_BYTE_ARRAY_TYPE, DataTypes.BinaryType);
    }

    private TypeConverterUtils() {
        throw new UnsupportedOperationException("TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static DataType convert(SeaTunnelDataType<?> dataType) {
        DataType sparkType = SEA_TUNNEL_TO_SPARK_TYPES.get(dataType);
        if (sparkType != null) {
            return sparkType;
        }
        if (dataType instanceof TimestampType) {
            return DataTypes.TimestampType;
        }
        if (dataType instanceof ArrayType) {
            return createArrayType(((ArrayType<?, ?>) dataType).getElementType());
        }
        if (dataType instanceof ListType) {
            return createArrayType(((ListType<?>) dataType).getElementType());
        }
        if (dataType instanceof PojoType) {
            return new ObjectType(dataType.getTypeClass());
        }
        if (dataType instanceof SeaTunnelRowType) {
            return convert((SeaTunnelRowType) dataType);
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    private static org.apache.spark.sql.types.ArrayType createArrayType(SeaTunnelDataType<?> dataType) {
        return DataTypes.createArrayType(convert(dataType));
    }

    private static StructType convert(SeaTunnelRowType rowType) {
        StructField[] fields = new StructField[rowType.getFieldNames().length];
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            fields[i] = new StructField(rowType.getFieldNames()[i],
                convert(rowType.getFieldTypes()[i]), true, Metadata.empty());
        }
        return new StructType(fields);
    }
}

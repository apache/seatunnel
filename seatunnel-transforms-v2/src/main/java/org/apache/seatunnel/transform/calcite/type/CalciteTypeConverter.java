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

package org.apache.seatunnel.transform.calcite.type;

import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.sql.type.SqlTypeName;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.exception.TransformCommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Bidirectional type converter between {@link SeaTunnelDataType} and Calcite {@link RelDataType}.
 * Covers all types defined in {@link SqlType}.
 */
@UtilityClass
public final class CalciteTypeConverter {

    /**
     * Converts a SeaTunnel type to the corresponding Calcite RelDataType.
     *
     * @param factory Calcite type factory
     * @param seaTunnelType SeaTunnel data type
     * @return the corresponding Calcite type
     */
    public static RelDataType toCalciteType(
            RelDataTypeFactory factory, SeaTunnelDataType<?> seaTunnelType) {
        SqlType sqlType = seaTunnelType.getSqlType();
        switch (sqlType) {
            case BOOLEAN:
                return factory.createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return factory.createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return factory.createSqlType(SqlTypeName.SMALLINT);
            case INT:
                return factory.createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return factory.createSqlType(SqlTypeName.REAL);
            case DOUBLE:
                return factory.createSqlType(SqlTypeName.DOUBLE);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) seaTunnelType;
                return factory.createSqlType(
                        SqlTypeName.DECIMAL, decimalType.getPrecision(), decimalType.getScale());
            case STRING:
                return factory.createSqlType(SqlTypeName.VARCHAR);
            case BYTES:
                return factory.createSqlType(SqlTypeName.VARBINARY);
            case DATE:
                return factory.createSqlType(SqlTypeName.DATE);
            case TIME:
                return factory.createSqlType(SqlTypeName.TIME, 3);
            case TIMESTAMP:
                return factory.createSqlType(SqlTypeName.TIMESTAMP);
            case TIMESTAMP_TZ:
                return factory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            case NULL:
                return factory.createSqlType(SqlTypeName.NULL);
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) seaTunnelType;
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                RelDataType calciteElementType = toCalciteType(factory, elementType);
                return factory.createArrayType(calciteElementType, -1);
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) seaTunnelType;
                RelDataType keyType = toCalciteType(factory, mapType.getKeyType());
                RelDataType valueType = toCalciteType(factory, mapType.getValueType());
                return factory.createMapType(keyType, valueType);
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) seaTunnelType;
                List<String> fieldNames = new ArrayList<>();
                List<RelDataType> fieldTypes = new ArrayList<>();
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    fieldNames.add(rowType.getFieldName(i));
                    fieldTypes.add(toCalciteType(factory, rowType.getFieldType(i)));
                }
                return factory.createStructType(fieldTypes, fieldNames);
            case MULTIPLE_ROW:
                return factory.createSqlType(SqlTypeName.ANY);
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
            case SPARSE_FLOAT_VECTOR:
                return factory.createSqlType(SqlTypeName.VARBINARY);
            default:
                throw new TransformException(
                        TransformCommonErrorCode.EXPRESSION_EXECUTE_ERROR,
                        "Unsupported SeaTunnel type for Calcite mapping: " + sqlType);
        }
    }

    /**
     * Converts a Calcite RelDataType back to the corresponding SeaTunnel type.
     *
     * @param calciteType Calcite type
     * @return the corresponding SeaTunnel data type
     */
    public static SeaTunnelDataType<?> toSeaTunnelType(RelDataType calciteType) {
        SqlTypeName typeName = calciteType.getSqlTypeName();
        switch (typeName) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case REAL:
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DECIMAL:
                return new DecimalType(calciteType.getPrecision(), calciteType.getScale());
            case CHAR:
            case VARCHAR:
                return BasicType.STRING_TYPE;
            case BINARY:
            case VARBINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.OFFSET_DATE_TIME_TYPE;
            case NULL:
                return BasicType.VOID_TYPE;
            case ANY:
                return BasicType.STRING_TYPE;
            case ARRAY:
            case MULTISET:
                RelDataType componentType = calciteType.getComponentType();
                if (componentType == null) {
                    return ArrayType.STRING_ARRAY_TYPE;
                }
                SeaTunnelDataType<?> stElementType = toSeaTunnelType(componentType);
                return convertToArrayType(stElementType);
            case MAP:
                RelDataType keyRelType = calciteType.getKeyType();
                RelDataType valueRelType = calciteType.getValueType();
                if (keyRelType == null || valueRelType == null) {
                    return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
                }
                return new MapType<>(toSeaTunnelType(keyRelType), toSeaTunnelType(valueRelType));
            case ROW:
                return convertStructToRowType(calciteType);
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return BasicType.LONG_TYPE;
            default:
                if (calciteType.isStruct()) {
                    return convertStructToRowType(calciteType);
                }
                throw new TransformException(
                        TransformCommonErrorCode.EXPRESSION_EXECUTE_ERROR,
                        "Unsupported Calcite type for SeaTunnel mapping: " + typeName);
        }
    }

    private static SeaTunnelRowType convertStructToRowType(RelDataType structType) {
        List<String> names = new ArrayList<>();
        List<SeaTunnelDataType<?>> types = new ArrayList<>();
        structType
                .getFieldList()
                .forEach(
                        field -> {
                            names.add(field.getName());
                            types.add(toSeaTunnelType(field.getType()));
                        });
        return new SeaTunnelRowType(
                names.toArray(new String[0]), types.toArray(new SeaTunnelDataType[0]));
    }

    private static ArrayType<?, ?> convertToArrayType(SeaTunnelDataType<?> elementType) {
        SqlType sqlType = elementType.getSqlType();
        switch (sqlType) {
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
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) elementType;
                return new ArrayType<>(MapType.class, mapType);
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) elementType;
                return ArrayType.of(arrayType);
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case ROW:
            case BYTES:
                return new ArrayType<>(elementType.getTypeClass(), elementType);
            default:
                return ArrayType.STRING_ARRAY_TYPE;
        }
    }
}

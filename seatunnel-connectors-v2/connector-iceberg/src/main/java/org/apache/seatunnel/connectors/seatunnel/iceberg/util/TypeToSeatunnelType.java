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

package org.apache.seatunnel.connectors.seatunnel.iceberg.util;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.api.table.type.BasicType.BOOLEAN_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

class TypeToSeatunnelType extends TypeUtil.SchemaVisitor<SeaTunnelDataType<?>> {
    TypeToSeatunnelType() {}

    @Override
    public SeaTunnelDataType<?> schema(Schema schema, SeaTunnelDataType<?> structType) {
        return structType;
    }

    @Override
    public SeaTunnelDataType<?> struct(
            Types.StructType struct, List<SeaTunnelDataType<?>> fieldResults) {
        checkArgument(
                struct.fields().size() == fieldResults.size(),
                "Expected %s field results for struct type %s, but found %s",
                struct.fields().size(),
                struct,
                fieldResults.size());
        List<String> fieldNames =
                struct.fields().stream().map(Types.NestedField::name).collect(Collectors.toList());

        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                fieldResults.toArray(new SeaTunnelDataType<?>[0]));
    }

    @Override
    public SeaTunnelDataType<?> field(Types.NestedField field, SeaTunnelDataType<?> fieldResult) {
        return fieldResult;
    }

    @Override
    public SeaTunnelDataType<?> list(Types.ListType list, SeaTunnelDataType<?> elementResult) {
        throw new UnsupportedOperationException(
                "Cannot convert unknown type to SeaTunnel: " + list);
    }

    @Override
    public SeaTunnelDataType<?> map(
            Types.MapType map, SeaTunnelDataType<?> keyResult, SeaTunnelDataType<?> valueResult) {
        throw new UnsupportedOperationException("Cannot convert unknown type to SeaTunnel: " + map);
    }

    @Override
    public SeaTunnelDataType<?> primitive(Type.PrimitiveType primitive) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return BOOLEAN_TYPE;
            case INTEGER:
                return INT_TYPE;
            case LONG:
                return LONG_TYPE;
            case FLOAT:
                return FLOAT_TYPE;
            case DOUBLE:
                return DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case STRING:
                return STRING_TYPE;
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return new DecimalType(decimal.precision(), decimal.scale());
            default:
                throw new UnsupportedOperationException(
                        "Cannot convert unknown type to SeaTunnel: " + primitive);
        }
    }
}

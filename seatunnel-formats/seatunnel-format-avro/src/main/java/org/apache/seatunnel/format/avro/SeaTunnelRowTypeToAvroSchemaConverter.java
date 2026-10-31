/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.avro.exception.AvroFormatErrorCode;
import org.apache.seatunnel.format.avro.exception.SeaTunnelAvroFormatException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class SeaTunnelRowTypeToAvroSchemaConverter {

    /**
     * Builds the Avro record that describes the given row type with the legacy layout, where a ROW
     * field is a bare Avro record and therefore cannot hold a null value.
     *
     * @param seaTunnelRowType row type to describe
     * @return the Avro record schema
     */
    public static Schema buildAvroSchemaWithRowType(SeaTunnelRowType seaTunnelRowType) {
        return buildAvroSchemaWithRowType(seaTunnelRowType, false);
    }

    /**
     * Builds the Avro record that describes the given row type.
     *
     * <p>The {@code rowNullable} flag decides how every ROW field is described, at any nesting
     * depth and no matter whether the ROW is a field, an ARRAY element or a MAP value. With {@code
     * false} a ROW keeps the legacy layout of a bare Avro record; with {@code true} each ROW is
     * wrapped in the union {@code [null, record]} so that a null ROW value can be written. The flag
     * is applied recursively, so a ROW nested several levels deep is described the same way as a
     * top level one.
     *
     * @param seaTunnelRowType row type to describe
     * @param rowNullable whether ROW fields are described as nullable unions
     * @return the Avro record schema
     */
    public static Schema buildAvroSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType, boolean rowNullable) {
        return buildRecordSchema("SeaTunnelRecord", seaTunnelRowType, rowNullable);
    }

    /**
     * Builds the Avro record that describes the given row type under an explicit record name.
     *
     * <p>The name is the one the enclosing schema assigned to the ROW: the field name for a ROW
     * field, and the enclosing field name for a ROW nested in an ARRAY or a MAP. A serialized
     * record must be built with the same name, otherwise Avro cannot resolve it against the union
     * branch of a nullable ROW.
     */
    static Schema buildRecordSchema(
            String recordName, SeaTunnelRowType seaTunnelRowType, boolean rowNullable) {
        List<Schema.Field> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(generateField(fieldNames[i], fieldTypes[i], rowNullable));
        }
        return Schema.createRecord(recordName, null, null, false, fields);
    }

    private static Schema.Field generateField(
            String fieldName, SeaTunnelDataType<?> seaTunnelDataType, boolean rowNullable) {
        return new Schema.Field(
                fieldName,
                seaTunnelDataType2AvroDataType(fieldName, seaTunnelDataType, rowNullable),
                null,
                null);
    }

    private static Schema seaTunnelDataType2AvroDataType(
            String fieldName, SeaTunnelDataType<?> seaTunnelDataType, boolean rowNullable) {

        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
            case BYTES:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES));
            case TINYINT:
            case SMALLINT:
            case INT:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
            case BIGINT:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
            case FLOAT:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
            case DOUBLE:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
            case BOOLEAN:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
            case MAP:
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) seaTunnelDataType).getValueType();
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createMap(
                                seaTunnelDataType2AvroDataType(fieldName, valueType, rowNullable)));
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.createArray(
                                seaTunnelDataType2AvroDataType(
                                        fieldName, elementType, rowNullable)));
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                List<Schema.Field> subField = new ArrayList<>();
                for (int i = 0; i < fieldNames.length; i++) {
                    subField.add(generateField(fieldNames[i], fieldTypes[i], rowNullable));
                }
                Schema recordSchema = Schema.createRecord(fieldName, null, null, false, subField);
                return rowNullable
                        ? Schema.createUnion(Schema.create(Schema.Type.NULL), recordSchema)
                        : recordSchema;
            case DECIMAL:
                int precision = ((DecimalType) seaTunnelDataType).getPrecision();
                int scale = ((DecimalType) seaTunnelDataType).getScale();
                LogicalTypes.Decimal decimal = LogicalTypes.decimal(precision, scale);
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        decimal.addToSchema(Schema.create(Schema.Type.BYTES)));
            case TIMESTAMP:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        LogicalTypes.localTimestampMillis()
                                .addToSchema(Schema.create(Schema.Type.LONG)));
            case DATE:
                return Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
            case NULL:
                return Schema.create(Schema.Type.NULL);
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new SeaTunnelAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}

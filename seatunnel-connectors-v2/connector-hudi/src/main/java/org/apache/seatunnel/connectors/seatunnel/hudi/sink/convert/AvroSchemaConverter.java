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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Converts an Avro schema into Seatunnel's type information. */
public class AvroSchemaConverter implements Serializable {

    private AvroSchemaConverter() {
        // private
    }

    /**
     * Converts Seatunnel {@link SeaTunnelDataType} (can be nested) into an Avro schema.
     *
     * <p>Use "org.apache.seatunnel.avro.generated.record" as the type name.
     *
     * @param schema the schema type, usually it should be the top level record type, e.g. not a
     *     nested type
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(SeaTunnelDataType<?> schema) {
        return convertToSchema(schema, "record");
    }

    /**
     * Converts Seatunnel {@link SeaTunnelDataType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}." is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param dataType logical type
     * @param rowName the record name
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(SeaTunnelDataType<?> dataType, String rowName) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                Schema bool = SchemaBuilder.builder().booleanType();
                return nullableSchema(bool);
            case TINYINT:
            case SMALLINT:
            case INT:
                Schema integer = SchemaBuilder.builder().intType();
                return nullableSchema(integer);
            case BIGINT:
                Schema bigint = SchemaBuilder.builder().longType();
                return nullableSchema(bigint);
            case FLOAT:
                Schema f = SchemaBuilder.builder().floatType();
                return nullableSchema(f);
            case DOUBLE:
                Schema d = SchemaBuilder.builder().doubleType();
                return nullableSchema(d);
            case STRING:
                Schema str = SchemaBuilder.builder().stringType();
                return nullableSchema(str);
            case BYTES:
                Schema binary = SchemaBuilder.builder().bytesType();
                return nullableSchema(binary);
            case TIMESTAMP:
                // use long to represents Timestamp
                LogicalType avroLogicalType;
                avroLogicalType = LogicalTypes.timestampMillis();
                Schema timestamp = avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
                return nullableSchema(timestamp);
            case DATE:
                // use int to represents Date
                Schema date = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                return nullableSchema(date);
            case TIME:
                // use int to represents Time, we only support millisecond when deserialization
                Schema time =
                        LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
                return nullableSchema(time);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                // store BigDecimal as Fixed
                // for spark compatibility.
                Schema decimal =
                        LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                                .addToSchema(
                                        SchemaBuilder.fixed(String.format("%s.fixed", rowName))
                                                .size(
                                                        computeMinBytesForDecimalPrecision(
                                                                decimalType.getPrecision())));
                return nullableSchema(decimal);
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                List<String> fieldNames = Arrays.asList(rowType.getFieldNames());
                // we have to make sure the record name is different in a Schema
                SchemaBuilder.FieldAssembler<Schema> builder =
                        SchemaBuilder.builder().record(rowName).fields();
                for (int i = 0; i < fieldNames.size(); i++) {
                    String fieldName = fieldNames.get(i);
                    SeaTunnelDataType<?> fieldType = rowType.getFieldType(i);
                    SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                            builder.name(fieldName)
                                    .type(convertToSchema(fieldType, rowName + "." + fieldName));

                    builder = fieldBuilder.withDefault(null);
                }
                return builder.endRecord();
            case MAP:
                Schema map =
                        SchemaBuilder.builder()
                                .map()
                                .values(
                                        convertToSchema(
                                                extractValueTypeToAvroMap(dataType), rowName));
                return nullableSchema(map);
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) dataType;
                Schema array =
                        SchemaBuilder.builder()
                                .array()
                                .items(convertToSchema(arrayType.getElementType(), rowName));
                return nullableSchema(array);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + dataType);
        }
    }

    public static SeaTunnelDataType<?> extractValueTypeToAvroMap(SeaTunnelDataType<?> type) {
        SeaTunnelDataType<?> keyType;
        SeaTunnelDataType<?> valueType;
        MapType<?, ?> mapType = (MapType<?, ?>) type;
        keyType = mapType.getKeyType();
        valueType = mapType.getValueType();
        if (keyType.getSqlType() != SqlType.STRING) {
            throw new UnsupportedOperationException(
                    "Avro format doesn't support non-string as key type of map. "
                            + "The key type is: "
                            + keyType.getSqlType());
        }
        return valueType;
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
    }

    private static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }
}

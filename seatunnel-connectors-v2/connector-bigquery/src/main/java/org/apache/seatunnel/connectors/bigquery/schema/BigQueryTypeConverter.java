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

package org.apache.seatunnel.connectors.bigquery.schema;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import com.google.cloud.bigquery.StandardSQLTypeName;

import java.util.ArrayList;
import java.util.List;

final class BigQueryTypeConverter {
    private static final int NUMERIC_MAX_PRECISION = 38;
    private static final int NUMERIC_MAX_SCALE = 9;
    private static final int NUMERIC_MAX_INTEGER_DIGITS = 29;
    private static final int BIGNUMERIC_MAX_PRECISION = 76;
    private static final int BIGNUMERIC_MAX_SCALE = 38;
    private static final int BIGNUMERIC_MAX_INTEGER_DIGITS = 38;

    private BigQueryTypeConverter() {}

    static String toDdlType(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return "BOOL";
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return "INT64";
            case FLOAT:
            case DOUBLE:
                return "FLOAT64";
            case DECIMAL:
                return decimalDdlType((DecimalType) dataType);
            case STRING:
                return "STRING";
            case BYTES:
                return "BYTES";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return "TIMESTAMP";
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) dataType).getElementType();
                if (elementType.getSqlType() == org.apache.seatunnel.api.table.type.SqlType.ARRAY) {
                    throw unsupported(dataType, "BigQuery does not support nested arrays");
                }
                return "ARRAY<" + toDdlType(elementType) + ">";
            case ROW:
                return rowDdlType((SeaTunnelRowType) dataType);
            default:
                throw unsupported(dataType, "Unsupported SeaTunnel type");
        }
    }

    static StandardSQLTypeName toStandardType(SeaTunnelDataType<?> dataType) {
        if (dataType instanceof ArrayType) {
            return toStandardType(((ArrayType<?, ?>) dataType).getElementType());
        }
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return StandardSQLTypeName.INT64;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case DECIMAL:
                return decimalStandardType((DecimalType) dataType);
            case STRING:
                return StandardSQLTypeName.STRING;
            case BYTES:
                return StandardSQLTypeName.BYTES;
            case DATE:
                return StandardSQLTypeName.DATE;
            case TIME:
                return StandardSQLTypeName.TIME;
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return StandardSQLTypeName.TIMESTAMP;
            case ROW:
                return StandardSQLTypeName.STRUCT;
            default:
                throw unsupported(dataType, "Unsupported SeaTunnel type");
        }
    }

    private static String decimalDdlType(DecimalType decimalType) {
        StandardSQLTypeName standardType = decimalStandardType(decimalType);
        return String.format(
                "%s(%d, %d)",
                standardType.name(), decimalType.getPrecision(), decimalType.getScale());
    }

    private static StandardSQLTypeName decimalStandardType(DecimalType decimalType) {
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        if (precision <= 0 || scale < 0 || scale > precision) {
            throw unsupported(decimalType, "Invalid decimal precision or scale");
        }
        if (precision <= NUMERIC_MAX_PRECISION
                && scale <= NUMERIC_MAX_SCALE
                && precision - scale <= NUMERIC_MAX_INTEGER_DIGITS) {
            return StandardSQLTypeName.NUMERIC;
        }
        if (precision <= BIGNUMERIC_MAX_PRECISION
                && scale <= BIGNUMERIC_MAX_SCALE
                && precision - scale <= BIGNUMERIC_MAX_INTEGER_DIGITS) {
            return StandardSQLTypeName.BIGNUMERIC;
        }
        throw unsupported(decimalType, "Decimal precision or scale exceeds BigQuery limits");
    }

    private static String rowDdlType(SeaTunnelRowType rowType) {
        List<String> fields = new ArrayList<>(rowType.getTotalFields());
        for (int index = 0; index < rowType.getTotalFields(); index++) {
            fields.add(
                    BigQuerySchemaChangeManager.quoteIdentifier(rowType.getFieldName(index))
                            + " "
                            + toDdlType(rowType.getFieldType(index)));
        }
        return "STRUCT<" + String.join(", ", fields) + ">";
    }

    private static BigQueryConnectorException unsupported(
            SeaTunnelDataType<?> dataType, String reason) {
        return new BigQueryConnectorException(
                BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                String.format("%s: %s (%s)", reason, dataType, dataType.getSqlType()));
    }
}

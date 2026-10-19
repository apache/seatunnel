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
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.translation.spark.exception.SparkTranslationErrorCode;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.regex.Pattern;

/**
 * Carries SeaTunnel {@code DECIMAL(p, s)} with {@code p > 38} through Spark, whose decimal type
 * stops at precision 38.
 *
 * <p>A wide decimal at the top level or inside a {@code ROW} becomes a Spark {@code StringType}
 * field with the declared precision and scale in the field metadata. The value is its plain decimal
 * text. A wide decimal inside an {@code ARRAY} or {@code MAP} has no field to hold the metadata, so
 * it is rejected before the job reads any data.
 */
public final class WideDecimalUtils {

    public static final int SPARK_MAX_PRECISION = 38;
    public static final String PRECISION_KEY = "seatunnel_decimal_precision";
    public static final String SCALE_KEY = "seatunnel_decimal_scale";

    private static final Pattern PLAIN_DECIMAL = Pattern.compile("-?\\d+(\\.\\d+)?");
    private static final int MAX_VALUE_LENGTH_IN_MESSAGE = 64;

    private WideDecimalUtils() {
        throw new UnsupportedOperationException(
                "WideDecimalUtils is a utility class and cannot be instantiated");
    }

    public static boolean isWideDecimal(SeaTunnelDataType<?> dataType) {
        return dataType instanceof DecimalType
                && ((DecimalType) dataType).getPrecision() > SPARK_MAX_PRECISION;
    }

    public static Metadata metadata(DecimalType decimalType) {
        return new MetadataBuilder()
                .putLong(PRECISION_KEY, decimalType.getPrecision())
                .putLong(SCALE_KEY, decimalType.getScale())
                .build();
    }

    /**
     * Restores the SeaTunnel type of a carrier field.
     *
     * @return the decimal type, or {@code null} if the field carries no decimal metadata
     */
    public static DecimalType fromMetadata(StructField field) {
        Metadata metadata = field.metadata();
        boolean hasPrecision = metadata != null && metadata.contains(PRECISION_KEY);
        boolean hasScale = metadata != null && metadata.contains(SCALE_KEY);
        if (!hasPrecision && !hasScale) {
            return null;
        }
        if (!hasPrecision || !hasScale) {
            throw invalidMetadata(field, "both keys must be present");
        }
        if (!DataTypes.StringType.equals(field.dataType())) {
            throw invalidMetadata(field, "the field type must be STRING");
        }
        long precision;
        long scale;
        try {
            precision = metadata.getLong(PRECISION_KEY);
            scale = metadata.getLong(SCALE_KEY);
        } catch (RuntimeException e) {
            throw invalidMetadata(field, "both values must be integers");
        }
        if (precision <= SPARK_MAX_PRECISION
                || precision > Integer.MAX_VALUE
                || scale < 0
                || scale > precision) {
            throw invalidMetadata(
                    field,
                    String.format(
                            "precision must be above %d and scale between 0 and precision",
                            SPARK_MAX_PRECISION));
        }
        return new DecimalType((int) precision, (int) scale);
    }

    /**
     * Fails if a wide decimal sits inside an {@code ARRAY} or {@code MAP} anywhere in the row type.
     * Call it once per source table, before the tables are merged, so that the error can name the
     * real table and column.
     *
     * @param tablePath the table path for the error message, or {@code null} if unknown
     */
    public static void checkNoWideDecimalInCollection(String tablePath, SeaTunnelRowType rowType) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            check(tablePath, rowType.getFieldName(i), rowType.getFieldType(i), false);
        }
    }

    private static void check(
            String tablePath, String path, SeaTunnelDataType<?> dataType, boolean inCollection) {
        switch (dataType.getSqlType()) {
            case DECIMAL:
                if (inCollection && isWideDecimal(dataType)) {
                    throw collectionError(tablePath, path, (DecimalType) dataType);
                }
                return;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    check(
                            tablePath,
                            path + "." + rowType.getFieldName(i),
                            rowType.getFieldType(i),
                            inCollection);
                }
                return;
            case ARRAY:
                check(
                        tablePath,
                        path + ".element",
                        ((ArrayType<?, ?>) dataType).getElementType(),
                        true);
                return;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                check(tablePath, path + ".key", mapType.getKeyType(), true);
                check(tablePath, path + ".value", mapType.getValueType(), true);
                return;
            default:
        }
    }

    /** SeaTunnel value to carrier text. Pads the scale but never rounds or narrows. */
    public static String toCarrierText(BigDecimal value, DecimalType decimalType, String column) {
        BigDecimal scaled;
        try {
            scaled = value.setScale(decimalType.getScale(), RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw invalidValue(
                    column,
                    decimalType,
                    value.toPlainString(),
                    "it has more than " + decimalType.getScale() + " fraction digits");
        }
        if (scaled.precision() > decimalType.getPrecision()) {
            throw invalidValue(
                    column,
                    decimalType,
                    value.toPlainString(),
                    "it has more than " + decimalType.getPrecision() + " digits");
        }
        return scaled.toPlainString();
    }

    /** Carrier text to SeaTunnel value. Requires the exact declared scale and never rescales. */
    public static BigDecimal fromCarrierText(String text, DecimalType decimalType, String column) {
        if (!PLAIN_DECIMAL.matcher(text).matches()) {
            throw invalidValue(column, decimalType, text, "it is not plain decimal text");
        }
        BigDecimal value = new BigDecimal(text);
        if (value.scale() != decimalType.getScale()) {
            throw invalidValue(
                    column,
                    decimalType,
                    text,
                    "its scale is " + value.scale() + ", expected " + decimalType.getScale());
        }
        if (value.precision() > decimalType.getPrecision()) {
            throw invalidValue(
                    column,
                    decimalType,
                    text,
                    "it has more than " + decimalType.getPrecision() + " digits");
        }
        return value;
    }

    static String typeName(DecimalType decimalType) {
        return String.format("DECIMAL(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
    }

    static SeaTunnelRuntimeException collectionError(
            String tablePath, String columnPath, DecimalType decimalType) {
        String table = tablePath == null ? "" : String.format(" of table `%s`", tablePath);
        return new SeaTunnelRuntimeException(
                SparkTranslationErrorCode.WIDE_DECIMAL_IN_COLLECTION,
                String.format(
                        "Column `%s`%s has type %s inside an ARRAY/MAP. "
                                + "Spark supports DECIMAL precision up to %d, "
                                + "and SeaTunnel cannot carry wider decimals inside ARRAY/MAP on Spark. "
                                + "Workarounds: leave the column out at the source "
                                + "(e.g. Doris `doris.read.field`, or a JDBC `query` that omits it), "
                                + "cast it to a string in the source query, "
                                + "or run this job on the Zeta or Flink engine.",
                        columnPath, table, typeName(decimalType), SPARK_MAX_PRECISION));
    }

    private static InvalidWideDecimalException invalidValue(
            String column, DecimalType decimalType, String value, String reason) {
        return new InvalidWideDecimalException(column, decimalType, abbreviate(value), reason);
    }

    private static SeaTunnelRuntimeException invalidMetadata(StructField field, String reason) {
        return new SeaTunnelRuntimeException(
                SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_METADATA,
                String.format(
                        "Field `%s` of type %s has metadata %s, but %s.",
                        field.name(), field.dataType().sql(), field.metadata().json(), reason));
    }

    /**
     * A wide decimal value that cannot be carried. The converters pass only the field name down and
     * each enclosing {@code ROW} adds its name on the way up, so the column path is built only when
     * a value fails.
     */
    public static final class InvalidWideDecimalException extends SeaTunnelRuntimeException {

        private static final long serialVersionUID = 1L;

        private final String column;
        private final DecimalType decimalType;
        private final String value;
        private final String reason;

        private InvalidWideDecimalException(
                String column, DecimalType decimalType, String value, String reason) {
            super(
                    SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_VALUE,
                    String.format(
                            "Column `%s` declared as %s: value '%s' is rejected because %s.",
                            column, typeName(decimalType), value, reason));
            this.column = column;
            this.decimalType = decimalType;
            this.value = value;
            this.reason = reason;
        }

        /** @return the same error with {@code parent} prepended to the column path */
        public InvalidWideDecimalException withParent(String parent) {
            if (parent == null) {
                return this;
            }
            InvalidWideDecimalException e =
                    new InvalidWideDecimalException(
                            column == null ? parent : parent + "." + column,
                            decimalType,
                            value,
                            reason);
            e.setStackTrace(getStackTrace());
            return e;
        }
    }

    private static String abbreviate(String value) {
        if (value.length() <= MAX_VALUE_LENGTH_IN_MESSAGE) {
            return value;
        }
        return value.substring(0, MAX_VALUE_LENGTH_IN_MESSAGE) + "...";
    }
}

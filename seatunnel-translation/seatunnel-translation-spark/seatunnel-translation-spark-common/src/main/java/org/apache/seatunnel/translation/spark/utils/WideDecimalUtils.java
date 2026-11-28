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
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
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

    /** The largest precision of Spark's {@code DecimalType}. */
    public static final int SPARK_MAX_PRECISION = 38;

    /**
     * Field metadata key holding the declared precision of a carrier field, stored as a {@code
     * long}. It is part of the Spark schema that users see, so treat the name as a stable contract.
     */
    public static final String PRECISION_KEY = "seatunnel_decimal_precision";

    /**
     * Field metadata key holding the declared scale of a carrier field, stored as a {@code long}.
     * It is part of the Spark schema that users see, so treat the name as a stable contract.
     */
    public static final String SCALE_KEY = "seatunnel_decimal_scale";

    private static final Pattern PLAIN_DECIMAL = Pattern.compile("-?\\d+(\\.\\d+)?");

    private WideDecimalUtils() {
        throw new UnsupportedOperationException(
                "WideDecimalUtils is a utility class and cannot be instantiated");
    }

    /**
     * @return {@code true} if {@code dataType} is a SeaTunnel {@code DECIMAL} wider than Spark
     *     supports, i.e. one that is carried as text
     */
    public static boolean isWideDecimal(SeaTunnelDataType<?> dataType) {
        return dataType instanceof DecimalType
                && ((DecimalType) dataType).getPrecision() > SPARK_MAX_PRECISION;
    }

    /**
     * @return the metadata of a carrier field: {@link #PRECISION_KEY} and {@link #SCALE_KEY}, both
     *     as {@code long}
     */
    public static Metadata metadata(DecimalType decimalType) {
        return new MetadataBuilder()
                .putLong(PRECISION_KEY, decimalType.getPrecision())
                .putLong(SCALE_KEY, decimalType.getScale())
                .build();
    }

    /**
     * Restores the SeaTunnel type of a carrier field when a Spark schema is converted back to a
     * SeaTunnel type. The job path does not use it: row values are restored from the SeaTunnel row
     * type that the converters hold.
     *
     * @return the decimal type, or {@code null} if the field carries no decimal metadata
     * @throws SeaTunnelRuntimeException with {@code SPARK-TRANSLATION-03} if only one key is
     *     present, the field is not a {@code STRING}, or the values are not a valid wide decimal
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
        // getLong would unbox a JSON null as 0, so check the stored values first.
        Object precisionValue = metadata.map().apply(PRECISION_KEY);
        Object scaleValue = metadata.map().apply(SCALE_KEY);
        if (!(precisionValue instanceof Long) || !(scaleValue instanceof Long)) {
            throw invalidMetadata(field, "both values must be integers");
        }
        long precision = (Long) precisionValue;
        long scale = (Long) scaleValue;
        if (precision <= SPARK_MAX_PRECISION
                || precision > Integer.MAX_VALUE
                || scale < Integer.MIN_VALUE
                || scale > Integer.MAX_VALUE) {
            // Any int scale is valid: negative (PostgreSQL 15+) and above the precision are both
            // legal, and the carrier handles them.
            throw invalidMetadata(
                    field,
                    String.format(
                            "precision must be above %d and both values must fit an int",
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
     * @throws SeaTunnelRuntimeException with {@code SPARK-TRANSLATION-01}
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
                            childPath(path, rowType.getFieldName(i)),
                            rowType.getFieldType(i),
                            inCollection);
                }
                return;
            case ARRAY:
                check(
                        tablePath,
                        childPath(path, "element"),
                        ((ArrayType<?, ?>) dataType).getElementType(),
                        true);
                return;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                check(tablePath, childPath(path, "key"), mapType.getKeyType(), true);
                check(tablePath, childPath(path, "value"), mapType.getValueType(), true);
                return;
            default:
        }
    }

    /** @return {@code child} under {@code parent}, or whichever of the two is not {@code null} */
    static String childPath(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return child == null ? parent : parent + "." + child;
    }

    /**
     * Converts a SeaTunnel value to carrier text. Pads the scale but never rounds or narrows.
     *
     * <p>The digit counts are checked on the unscaled value before anything is expanded, so a value
     * with an extreme exponent such as {@code 1E+100000000} fails at once instead of materialising
     * all its digits.
     *
     * @param column the column path for the error message; {@code null} inside an ARRAY or MAP
     * @throws InvalidWideDecimalException if the value has more fraction digits than the declared
     *     scale or more integer digits than {@code precision - scale}, or if its text would be
     *     longer than {@code Integer.MAX_VALUE / 4} characters
     */
    public static String toCarrierText(BigDecimal value, DecimalType decimalType, String column) {
        int scale = decimalType.getScale();
        if (value.signum() == 0) {
            checkTextLength(1, scale, decimalType, column);
            // setScale on a zero with a large negative scale would still build a power of ten.
            return scale > 0 ? BigDecimal.valueOf(0, scale).toPlainString() : "0";
        }
        // precision() - scale() is the number of integer digits; it can be zero or negative for
        // |value| < 1, which always fits.
        long integerDigits = (long) value.precision() - value.scale();
        long maxIntegerDigits = (long) decimalType.getPrecision() - scale;
        if (integerDigits > maxIntegerDigits) {
            throw invalidValue(
                    column,
                    decimalType,
                    "it has more than " + maxIntegerDigits + " integer digits");
        }
        checkTextLength(integerDigits, scale, decimalType, column);
        // Dropping d digits needs d trailing zeros in the unscaled value, so it needs more than d
        // digits and at least d trailing zero bits. Both checks are cheap and reject most values
        // that would need rounding before setScale divides. A value that passes them has at most
        // precision + d digits (the integer digits were checked), so once the divisor 10^d exists,
        // the division yields a quotient of at most `precision` digits.
        long droppedDigits = (long) value.scale() - scale;
        if (droppedDigits > 0
                && (droppedDigits >= value.precision()
                        || value.unscaledValue().getLowestSetBit() < droppedDigits)) {
            throw invalidValue(column, decimalType, tooManyFractionDigits(scale));
        }
        BigDecimal scaled;
        try {
            // Adds at most `precision` digits here, because the integer digits were checked.
            scaled = value.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw invalidValue(column, decimalType, tooManyFractionDigits(scale));
        }
        return scaled.toPlainString();
    }

    /**
     * The longest carrier text this class builds. It stays far below the largest Java string, even
     * when strings are stored as UTF-16, so the JDK's own buffers in {@code toPlainString} fit too.
     */
    private static final long MAX_TEXT_LENGTH = Integer.MAX_VALUE / 4;

    /**
     * Rejects a value whose plain text at the declared scale would be too long to build, before
     * anything is rendered.
     */
    private static void checkTextLength(
            long integerDigits, int scale, DecimalType decimalType, String column) {
        long length = 1 + Math.max(integerDigits, 1) + (scale > 0 ? 1L + scale : 0);
        if (length > MAX_TEXT_LENGTH) {
            throw invalidValue(column, decimalType, "its text would be too long to carry");
        }
    }

    /**
     * Converts carrier text back to a SeaTunnel value. Requires the exact declared scale and never
     * rounds: a negative scale is restored exactly, without dropping any non-zero digit.
     *
     * <p>The length is checked before anything else, so oversized text is rejected without being
     * parsed. {@link #toCarrierText} never writes leading zeros, so valid carrier text always fits
     * the bound.
     *
     * @param column the column path for the error message; {@code null} inside an ARRAY or MAP
     * @throws InvalidWideDecimalException if the text is not plain decimal text with exactly {@code
     *     scale} fraction digits (none for a negative scale, where it must be a multiple of {@code
     *     10^-scale}) whose unscaled value at the declared scale has at most {@code precision}
     *     digits
     */
    public static BigDecimal fromCarrierText(String text, DecimalType decimalType, String column) {
        if (text.length() > maxCarrierLength(decimalType)
                || !PLAIN_DECIMAL.matcher(text).matches()) {
            throw notCarrierText(column, decimalType);
        }
        int scale = decimalType.getScale();
        // A negative scale is written as a plain integer, so it expects no fraction digits.
        int expectedFractionDigits = Math.max(scale, 0);
        int point = text.indexOf('.');
        int fractionDigits = point < 0 ? 0 : text.length() - point - 1;
        if (fractionDigits != expectedFractionDigits) {
            throw invalidValue(
                    column,
                    decimalType,
                    "it has "
                            + fractionDigits
                            + " fraction digits, expected "
                            + expectedFractionDigits);
        }
        BigDecimal value;
        if (scale < 0) {
            value = withNegativeScale(text, decimalType, column);
        } else {
            checkDigitCount(text, text.length(), decimalType, column);
            value = new BigDecimal(text);
        }
        if (value.precision() > decimalType.getPrecision()) {
            throw invalidValue(
                    column,
                    decimalType,
                    "it has more than " + decimalType.getPrecision() + " digits");
        }
        return value;
    }

    /**
     * Restores a negative scale from plain integer text. A non-zero value must end in {@code
     * -scale} zeros; they are checked in the text and only the digits before them are parsed, so
     * neither the zeros nor a division by {@code 10^-scale} cost anything.
     */
    private static BigDecimal withNegativeScale(
            String text, DecimalType decimalType, String column) {
        int scale = decimalType.getScale();
        int firstDigit = text.startsWith("-") ? 1 : 0;
        int lastNonZero = text.length() - 1;
        while (lastNonZero >= firstDigit && text.charAt(lastNonZero) == '0') {
            lastNonZero--;
        }
        if (lastNonZero < firstDigit) {
            return BigDecimal.valueOf(0, scale);
        }
        long trailingZeros = text.length() - 1 - lastNonZero;
        if (trailingZeros < -(long) scale) {
            throw invalidValue(column, decimalType, tooManyFractionDigits(scale));
        }
        // trailingZeros >= -scale, so this is at least lastNonZero + 1 and fits an int.
        int digitsEnd = (int) (text.length() + (long) scale);
        checkDigitCount(text, digitsEnd, decimalType, column);
        return new BigDecimal(new BigInteger(text.substring(0, digitsEnd)), scale);
    }

    /**
     * Counts the digits of the unscaled value in {@code text[0, end)}, from the first non-zero
     * digit on, and rejects more than {@code precision} of them before anything is parsed.
     */
    private static void checkDigitCount(
            String text, int end, DecimalType decimalType, String column) {
        int digits = 0;
        boolean significant = false;
        for (int i = 0; i < end; i++) {
            char c = text.charAt(i);
            if (c >= '1' && c <= '9') {
                significant = true;
            }
            if (significant && c != '.') {
                digits++;
            }
        }
        if (digits > decimalType.getPrecision()) {
            throw invalidValue(
                    column,
                    decimalType,
                    "it has more than " + decimalType.getPrecision() + " digits");
        }
    }

    /**
     * Converts the value Spark holds in a carrier field back to a SeaTunnel value. Only carrier
     * text ({@code String} or {@code UTF8String}) is accepted.
     *
     * @param column the column path for the error message; {@code null} inside an ARRAY or MAP
     * @throws InvalidWideDecimalException if the value is not valid carrier text, see {@link
     *     #fromCarrierText}
     */
    public static BigDecimal fromSparkValue(Object value, DecimalType decimalType, String column) {
        if (value instanceof UTF8String) {
            // Check the byte length before decoding the whole string. Carrier text is ASCII, so
            // its byte length equals its length.
            if (((UTF8String) value).numBytes() > maxCarrierLength(decimalType)) {
                throw notCarrierText(column, decimalType);
            }
            return fromCarrierText(value.toString(), decimalType, column);
        }
        if (value instanceof String) {
            return fromCarrierText((String) value, decimalType, column);
        }
        throw invalidValue(
                column,
                decimalType,
                "Spark holds a " + value.getClass().getName() + " instead of the STRING carrier");
    }

    /**
     * An upper bound on the length of carrier text that {@link #toCarrierText} writes: sign, every
     * digit of precision and scale (a scale above the precision adds leading fraction zeros), "0"
     * and the point. It only keeps parsing bounded; the exact checks come after it.
     */
    private static long maxCarrierLength(DecimalType decimalType) {
        // A negative scale adds that many integer zeros, so count its absolute value.
        return 3L + decimalType.getPrecision() + Math.abs((long) decimalType.getScale());
    }

    private static InvalidWideDecimalException notCarrierText(
            String column, DecimalType decimalType) {
        return invalidValue(
                column,
                decimalType,
                "it is not plain decimal text that fits " + typeName(decimalType));
    }

    private static String tooManyFractionDigits(int scale) {
        return scale >= 0
                ? "it has more than " + scale + " fraction digits"
                : "it is not a multiple of 1E+" + (-(long) scale);
    }

    static String typeName(DecimalType decimalType) {
        return String.format("DECIMAL(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
    }

    /**
     * @param tablePath the table, or {@code null} when the caller does not know it
     * @param columnPath the column path, e.g. {@code tags.element}
     */
    static SeaTunnelRuntimeException collectionError(
            String tablePath, String columnPath, DecimalType decimalType) {
        String table =
                tablePath == null
                        ? " (the table is not known at this point)"
                        : String.format(" of table `%s`", tablePath);
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
            String column, DecimalType decimalType, String reason) {
        return new InvalidWideDecimalException(null, column, null, decimalType, reason);
    }

    private static SeaTunnelRuntimeException invalidMetadata(StructField field, String reason) {
        return new SeaTunnelRuntimeException(
                SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_METADATA,
                // Not metadata.json(): Spark 2.4 fails to serialize a null value.
                String.format(
                        "Field `%s` of type %s carries wide decimal metadata (%s, %s), but %s.",
                        field.name(), field.dataType().sql(), PRECISION_KEY, SCALE_KEY, reason));
    }

    /**
     * A wide decimal value that cannot be carried ({@code SPARK-TRANSLATION-02}). The message names
     * the column, the declared type and the reason, but never the value itself.
     *
     * <p>The converters pass only the field name down and each enclosing {@code ROW} adds its name
     * on the way up, so the column path is built only when a value fails. The converter that knows
     * the source table adds it last.
     */
    public static final class InvalidWideDecimalException extends SeaTunnelRuntimeException {

        private static final long serialVersionUID = 1L;

        private final String table;
        // The column path is root.nested; the root is kept apart so withSource can replace it
        // even when a column name contains a dot.
        private final String root;
        private final String nested;
        private final DecimalType decimalType;
        private final String reason;

        private InvalidWideDecimalException(
                String table, String root, String nested, DecimalType decimalType, String reason) {
            super(
                    SparkTranslationErrorCode.INVALID_WIDE_DECIMAL_VALUE,
                    message(table, childPath(root, nested), decimalType, reason));
            this.table = table;
            this.root = root;
            this.nested = nested;
            this.decimalType = decimalType;
            this.reason = reason;
        }

        private static String message(
                String table, String column, DecimalType decimalType, String reason) {
            return String.format(
                    "%s%s declared as %s: the value is rejected because %s.",
                    column == null ? "An ARRAY/MAP element" : "Column `" + column + "`",
                    table == null ? "" : String.format(" of table `%s`", table),
                    typeName(decimalType),
                    reason);
        }

        /** @return the same error with {@code parent} prepended to the column path */
        public InvalidWideDecimalException withParent(String parent) {
            if (parent == null) {
                return this;
            }
            return copy(table, parent, childPath(root, nested));
        }

        /**
         * @param sourceTable the source table, or {@code null} if unknown
         * @param sourceColumn the source column name, replacing the top-level part of the path
         *     (which is the merged {@code columnN} name in a multi-table job), or {@code null} to
         *     keep it
         * @return the same error naming the source table and column
         */
        public InvalidWideDecimalException withSource(String sourceTable, String sourceColumn) {
            return copy(sourceTable, sourceColumn == null ? root : sourceColumn, nested);
        }

        private InvalidWideDecimalException copy(
                String newTable, String newRoot, String newNested) {
            InvalidWideDecimalException e =
                    new InvalidWideDecimalException(
                            newTable, newRoot, newNested, decimalType, reason);
            e.setStackTrace(getStackTrace());
            return e;
        }
    }
}

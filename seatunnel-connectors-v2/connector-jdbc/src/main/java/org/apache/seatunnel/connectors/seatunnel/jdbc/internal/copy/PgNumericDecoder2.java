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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.copy;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * High-performance decoder for PostgreSQL NUMERIC values in COPY binary format.
 *
 * <p>This decoder reconstructs NUMERIC values from base-10000 digit groups, following PostgreSQL’s
 * internal binary layout:
 *
 * <p>int16 ndigits -- number of base-10000 digit groups int16 weight -- weight of first digit group
 * int16 sign -- NUMERIC_POS, NUMERIC_NEG, NUMERIC_NAN, etc. int16 dscale -- number of decimal
 * digits after decimal point int16[] digits -- each group stores a value in [0, 9999]
 *
 * <p>The implementation avoids expensive operations (e.g., String.format), minimizes allocations,
 * and ensures exact numeric precision by manually constructing the final decimal string before
 * feeding it to BigDecimal.
 */
public final class PgNumericDecoder2 {

    /** PostgreSQL NUMERIC stores digits in base-10000 */
    private static final int NBASE = 10000;

    /** Number of decimal digits represented by each base-10000 group */
    private static final int DEC_DIGITS = 4;

    /** Positive finite number (normal numeric with value ≥ 0). */
    private static final int NUMERIC_POS = 0x0000;

    /** Negative finite number (normal numeric with value < 0). */
    private static final int NUMERIC_NEG = 0x4000;

    /**
     * Not-a-Number (NaN). PostgreSQL allows NUMERIC to represent NaN, but most engines (including
     * JDBC) do not support BigDecimal NaN; therefore, callers usually throw an exception.
     */
    private static final int NUMERIC_NAN = 0xC000;

    /**
     * Positive infinity (+∞). NUMERIC rarely stores infinities, but PostgreSQL's type system allows
     * it. Not representable in BigDecimal, so typically treated as unsupported.
     */
    private static final int NUMERIC_PINF = 0xD000;

    /** Negative infinity (−∞). Same handling as NUMERIC_PINF — cannot be mapped to BigDecimal. */
    private static final int NUMERIC_NINF = 0xF000;

    /** Mask for extracting dscale from the header */
    private static final int NUMERIC_DSCALE_MASK = 0x3FFF;

    private PgNumericDecoder2() {}

    /**
     * Decode a PostgreSQL NUMERIC value from a ByteBuffer positioned directly at the start of the
     * NUMERIC payload (ndigits field).
     */
    public static BigDecimal decode(ByteBuffer buf) {
        // PostgreSQL binary protocol uses big-endian; ByteBuffer default order matches.
        return parseNumeric(buf);
    }

    /** Parses the PostgreSQL NUMERIC format according to PostgreSQL’s on-disk representation. */
    private static BigDecimal parseNumeric(ByteBuffer buf) {

        // Read NUMERIC header
        int ndigits = Short.toUnsignedInt(buf.getShort()); // total number of base-10000 groups
        int weight = buf.getShort(); // weight of first group (can be negative)
        int sign = Short.toUnsignedInt(buf.getShort()); // special sign codes
        int dscale = Short.toUnsignedInt(buf.getShort()) & NUMERIC_DSCALE_MASK; // decimal scale

        // Handle special cases: NaN / ±Infinity (not supported)
        if (sign == NUMERIC_NAN) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "PostgreSQL NUMERIC value is NaN, not supported.");
        }
        if (sign == NUMERIC_PINF || sign == NUMERIC_NINF) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    "PostgreSQL NUMERIC Infinity not supported.");
        }

        // Read digit groups (each base-10000)
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            int dg = Short.toUnsignedInt(buf.getShort());
            if (dg < 0 || dg >= NBASE) {
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, "Invalid numeric digit: " + dg);
            }
            digits[i] = dg;
        }

        // Zero is represented as ndigits == 0
        if (ndigits == 0) {
            return BigDecimal.ZERO.setScale(dscale);
        }

        return buildBigDecimal(digits, ndigits, weight, sign, dscale);
    }

    /**
     * Reconstructs the exact decimal string for a NUMERIC value using integer and fractional
     * groups. This avoids any precision loss and is significantly faster than formatting groups
     * individually.
     */
    private static BigDecimal buildBigDecimal(
            int[] digits, int ndigits, int weight, int sign, int dscale) {

        // Pre-size StringBuilder to reduce resizes
        StringBuilder sb = new StringBuilder(32 + ndigits * DEC_DIGITS + dscale + 2);

        // Append sign if negative
        boolean negative = (sign == NUMERIC_NEG);
        if (negative) {
            sb.append('-');
        }

        // Number of digit groups that form the integer portion
        int intGroups = weight + 1;

        /* --------------------------
         *  Integer Part Construction
         * --------------------------
         * The first group has no leading zeros.
         * Remaining groups must be padded to 4 digits.
         */
        if (intGroups > 0) {

            // First digit group (no zero-padding)
            int firstGroup = (0 < ndigits) ? digits[0] : 0;
            appendIntWithoutLeadingZeros(sb, firstGroup);

            // Remaining integer groups (pad to exactly 4 digits)
            for (int gi = 1; gi < intGroups; gi++) {
                int groupVal = (gi < ndigits) ? digits[gi] : 0;
                appendPadded4(sb, groupVal);
            }

        } else {
            // Integer part is zero (e.g., 0.xxx)
            sb.append('0');
        }

        /* --------------------------
         *  Fractional Part (Scale)
         * -------------------------- */
        if (dscale > 0) {
            sb.append('.');
            int fracWritten = 0;

            // Leading fractional zeros when intGroups <= 0
            if (intGroups <= 0) {
                int leadingZeroDecimalDigits = (-intGroups) * DEC_DIGITS;
                int zerosToWrite = Math.min(leadingZeroDecimalDigits, dscale);
                appendZeros(sb, zerosToWrite);
                fracWritten += zerosToWrite;
            }

            // Append digits from subsequent groups
            int idx = Math.max(intGroups, 0);
            while (fracWritten < dscale && idx < ndigits) {
                int groupVal = digits[idx];
                char[] four = groupTo4Chars(groupVal);
                int need = Math.min(DEC_DIGITS, dscale - fracWritten);
                sb.append(four, 0, need);
                fracWritten += need;
                idx++;
            }

            // Pad trailing zeros if fractional digits are insufficient
            if (fracWritten < dscale) {
                appendZeros(sb, dscale - fracWritten);
            }
        }

        // Build BigDecimal from exact decimal string and apply scale explicitly
        BigDecimal bd = new BigDecimal(sb.toString());
        return bd.setScale(dscale);
    }

    /**
     * Appends an integer group without leading zeros. Used only for the most significant digit
     * group.
     */
    private static void appendIntWithoutLeadingZeros(StringBuilder sb, int value) {
        if (value == 0) {
            sb.append('0');
            return;
        }

        // Convert int → chars manually (avoids String.format)
        char[] buf = new char[4];
        int pos = 0;
        int v = value;

        // Build digits in reverse order
        while (v > 0) {
            buf[pos++] = (char) ('0' + (v % 10));
            v /= 10;
        }

        // Append in forward order
        for (int i = pos - 1; i >= 0; i--) {
            sb.append(buf[i]);
        }
    }

    /** Append a digit group padded to exactly 4 digits (e.g., "0001", "0325", "9999"). */
    private static void appendPadded4(StringBuilder sb, int value) {
        char[] four = groupTo4Chars(value);
        sb.append(four, 0, 4);
    }

    /** Converts a base-10000 group (0..9999) into four decimal characters without allocations. */
    private static char[] groupTo4Chars(int value) {
        char[] four = new char[4];
        int v = value;

        four[0] = (char) ('0' + (v / 1000));
        v %= 1000;
        four[1] = (char) ('0' + (v / 100));
        v %= 100;
        four[2] = (char) ('0' + (v / 10));
        four[3] = (char) ('0' + (v % 10));

        return four;
    }

    /** Append N '0' characters. */
    private static void appendZeros(StringBuilder sb, int count) {
        for (int i = 0; i < count; i++) {
            sb.append('0');
        }
    }

    /**
     * Decode a NUMERIC value in COPY format:
     *
     * <p>int32 length byte[length] payload (same as binary numeric format)
     *
     * <p>A length of -1 indicates NULL.
     */
    public static BigDecimal readCopyNumericColumn(ByteBuffer buf) {
        int len = buf.getInt();
        if (len == -1) {
            return null;
        }

        // Slice to isolate the numeric payload
        int start = buf.position();
        ByteBuffer payload = buf.slice();
        payload.limit(len);
        payload.order(buf.order());

        BigDecimal result = decode(payload);

        // Advance original buffer position beyond payload
        buf.position(start + len);
        return result;
    }
}

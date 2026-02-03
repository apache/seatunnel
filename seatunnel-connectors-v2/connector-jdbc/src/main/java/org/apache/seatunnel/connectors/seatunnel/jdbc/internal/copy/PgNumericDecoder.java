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

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

/**
 * PostgreSQL NUMERIC type decoder. Decodes PostgreSQL binary numeric representation into {@link
 * BigDecimal}.
 */
public final class PgNumericDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(PgNumericDecoder.class);

    private PgNumericDecoder() {}

    /**
     * Decodes PostgreSQL binary numeric format into BigDecimal
     *
     * @param buf ByteBuffer containing the binary numeric data
     * @return Decoded BigDecimal value
     * @throws JdbcConnectorException if the value is NaN
     */
    public static BigDecimal decode(ByteBuffer buf) {
        // ndigits: number of base-10000 digits
        int ndigits = buf.getShort() & 0xFFFF;
        int weight = buf.getShort(); // signed
        int sign = buf.getShort() & 0xFFFF; // 0x0000 positive, 0x4000 negative, 0xC000 NaN
        int dscale = buf.getShort() & 0xFFFF;

        // NaN is not supported
        if (sign == 0xC000) {
            return null; //
            //            throw new JdbcConnectorException(
            //                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
            //                    "PostgreSQL NUMERIC value is NaN, not supported.");
        }

        // Zero value (ndigits == 0)
        if (ndigits == 0) {
            BigDecimal zero = BigDecimal.ZERO.setScale(dscale);
            return zero;
        }

        // Read base-10000 digits
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = buf.getShort() & 0xFFFF; // value in 0..9999
        }

        // Build raw integer in base-10 by concatenating base-10000 groups
        BigInteger raw = BigInteger.ZERO;
        final BigInteger BASE = BigInteger.valueOf(10000);
        for (int d : digits) {
            raw = raw.multiply(BASE).add(BigInteger.valueOf(d));
        }

        // Compute base scale: fractional digits = (ndigits - (weight + 1)) * 4
        // This can be negative when intPart > ndigits
        int intPart = weight + 1;
        int baseScale = (ndigits - intPart) * 4;

        // Exact numeric value before applying declared dscale
        BigDecimal value = new BigDecimal(raw, baseScale);

        // Apply sign
        if (sign == 0x4000) {
            value = value.negate();
        }

        // Align to declared dscale:
        // - If baseScale < dscale -> pad trailing zeros
        // - If baseScale > dscale -> truncate extra fraction digits toward zero (match previous
        // behavior)
        if (value.scale() != dscale) {
            value = value.setScale(dscale, RoundingMode.DOWN);
        }

        //        if(value.equals(new BigDecimal("123.4500000000"))){
        //
        //            LOG.info("BigDecimal value: {}", value);
        //        }

        return value;
    }
}

/*
 * Example: PostgreSQL stores value: 123.45 (dscale=2), binary representation: ndigits=2, weight=0, sign=0, dscale=2
 *
 * digits = [123, 4500] (in base-10000 system)
 *
 * raw = 123×10000 + 4500 = 1234500
 *
 * baseScale = (2 - (0+1)) × 4 = 4
 *
 * value = new BigDecimal(1234500, 4) = 123.4500
 *
 * Set scale: 123.4500 → 123.45
 */

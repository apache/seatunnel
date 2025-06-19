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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class CharsetBasedSplitterTest {

    private static final String DEFAULT_CHARSET = "0123456789abcdefghijklmnopqrstuvwxyz";

    @Test
    @DisplayName("Test encoding of minimum and maximum values")
    public void testMinMax() {
        String minStr = "00000";
        String maxStr = "1";
        int maxLen = Math.max(minStr.length(), maxStr.length());
        String orderedCharset = "012a34b56789";
        BigInteger minBigInt =
                CollationBasedSplitter.encodeStringToNumericRange(
                        minStr, maxLen, true, true, orderedCharset, orderedCharset.length() + 1);
        log.info("Minimum value encoding: " + minBigInt);

        BigInteger maxBigInt =
                CollationBasedSplitter.encodeStringToNumericRange(
                        maxStr, maxLen, true, true, orderedCharset, orderedCharset.length() + 1);
        log.info("Maximum value encoding: " + maxBigInt);

        assert maxBigInt.compareTo(minBigInt) > 0;
    }

    @Test
    @DisplayName("Test consistency of string encoding and decoding")
    public void testEncodeDecode() {
        String original = "abc123";
        int maxLength = 10;
        boolean paddingAtEnd = true;
        boolean isCaseInsensitive = true;
        int radix = DEFAULT_CHARSET.length() + 1;

        BigInteger encoded =
                CollationBasedSplitter.encodeStringToNumericRange(
                        original,
                        maxLength,
                        paddingAtEnd,
                        isCaseInsensitive,
                        DEFAULT_CHARSET,
                        radix);

        String decoded =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encoded.toString(), maxLength, radix, DEFAULT_CHARSET);

        assertEquals(original.toLowerCase(), decoded.trim());
    }

    @Test
    @DisplayName("Test charset with special characters")
    public void testSpecialCharset() {
        String customCharset = "!@#$%^&*()_+-=[]{}|;:,.<>?";
        String input = "!@#$%";
        int maxLength = 10;
        int radix = customCharset.length() + 1;

        BigInteger encoded =
                CollationBasedSplitter.encodeStringToNumericRange(
                        input, maxLength, true, false, customCharset, radix);

        String decoded =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encoded.toString(), maxLength, radix, customCharset);

        assertEquals(input, decoded.trim());
    }

    @Test
    @DisplayName("Test impact of different padding positions")
    public void testPaddingPosition() {
        String input = "xyz";
        int maxLength = 5;
        int radix = DEFAULT_CHARSET.length() + 1;

        BigInteger encodedPrefix =
                CollationBasedSplitter.encodeStringToNumericRange(
                        input, maxLength, false, false, DEFAULT_CHARSET, radix);
        String decodedPrefix =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encodedPrefix.toString(), maxLength, radix, DEFAULT_CHARSET);

        BigInteger encodedSuffix =
                CollationBasedSplitter.encodeStringToNumericRange(
                        input, maxLength, true, false, DEFAULT_CHARSET, radix);
        String decodedSuffix =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encodedSuffix.toString(), maxLength, radix, DEFAULT_CHARSET);

        assertEquals(input, decodedPrefix.trim());
        assertEquals(input, decodedSuffix.trim());

        assert !encodedPrefix.equals(encodedSuffix);
    }

    @Test
    @DisplayName("Test performance")
    public void testPerformance() {
        int iterations = 1000;
        String input = "abcdefghijklmnopqrstuvwxyz";
        int maxLength = 30;
        int radix = DEFAULT_CHARSET.length() + 1;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            BigInteger encoded =
                    CollationBasedSplitter.encodeStringToNumericRange(
                            input, maxLength, true, true, DEFAULT_CHARSET, radix);

            String decoded =
                    CollationBasedSplitter.decodeNumericRangeToString(
                            encoded.toString(), maxLength, radix, DEFAULT_CHARSET);

            assertEquals(input, decoded.trim());
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        log.info(
                "Executing "
                        + iterations
                        + " encoding/decoding operations took: "
                        + duration
                        + " milliseconds");
        log.info("Average time per operation: " + (double) duration / iterations + " milliseconds");
    }

    @Test
    @DisplayName("Test encoding and decoding of random strings")
    public void testRandomStrings() {
        java.util.Random random = new java.util.Random();
        int testCount = 10;
        int maxLength = 20;
        int radix = DEFAULT_CHARSET.length() + 1;
        for (int test = 0; test < testCount; test++) {
            int length = random.nextInt(maxLength) + 1;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                int charIndex = random.nextInt(DEFAULT_CHARSET.length());
                sb.append(DEFAULT_CHARSET.charAt(charIndex));
            }
            String randomString = sb.toString();
            BigInteger encoded =
                    CollationBasedSplitter.encodeStringToNumericRange(
                            randomString, maxLength, true, false, DEFAULT_CHARSET, radix);

            String decoded =
                    CollationBasedSplitter.decodeNumericRangeToString(
                            encoded.toString(), maxLength, radix, DEFAULT_CHARSET);

            log.info("Random string #" + test + ": " + randomString);
            log.info("Encoding result: " + encoded);
            log.info("Decoding result: " + decoded.trim());

            assertEquals(randomString, decoded.trim());
        }
    }
}

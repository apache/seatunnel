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

import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.util.Arrays;

@Slf4j
public class CollationBasedSplitter {

    public static BigInteger encodeStringToNumericRange(
            String str,
            int maxLength,
            boolean paddingAtEnd,
            boolean isCaseInsensitive,
            String orderedCharset,
            int radix) {
        log.info(
                "Converting string '{}' to BigInteger, maxLength={}, isCaseInsensitive={}",
                str,
                maxLength,
                isCaseInsensitive);
        String asciiString =
                stringToAsciiString(
                        str, maxLength, paddingAtEnd, isCaseInsensitive, orderedCharset);
        log.info("String converted to ASCII representation: {}", asciiString);
        int[] baseArray = parseBaseNumber(asciiString);
        log.info("ASCII representation parsed to base array: {}", Arrays.toString(baseArray));
        BigInteger result = toDecimal(baseArray, radix);
        log.info("Final BigInteger result: {}", result);
        return result;
    }

    public static String decodeNumericRangeToString(
            String bigInteger, int maxLength, int radix, String orderedCharset) {
        log.info(
                "Converting BigInteger '{}' to string, maxLength={}, radix={}",
                bigInteger,
                maxLength,
                radix);
        int[] baseArray = fromDecimal(new BigInteger(bigInteger), maxLength, radix);
        log.info("BigInteger converted to base array: {}", Arrays.toString(baseArray));
        String formattedNumber = formatBaseNumber(baseArray);
        log.info("Base array formatted as number string: {}", formattedNumber);
        String result = convertToAsciiString(formattedNumber, orderedCharset);
        log.info("Final string result: '{}'", result);
        return result;
    }

    private static int[] parseBaseNumber(String numberStr) {
        log.trace("Parsing base number from string: {}", numberStr);
        String[] parts = numberStr.split(" ");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i]);
        }
        log.trace("Parsed base number result: {}", Arrays.toString(result));
        return result;
    }

    private static String formatBaseNumber(int[] number) {
        log.trace("Formatting base number array: {}", Arrays.toString(number));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < number.length; i++) {
            if (i > 0) sb.append(" ");
            sb.append(String.format("%03d", number[i]));
        }
        String result = sb.toString();
        log.trace("Formatted base number: {}", result);
        return result;
    }

    private static int charToIndex(char c, String supportedChars) {
        int result = (c == '\u0000') ? 0 : supportedChars.indexOf(c) + 1;
        log.trace("Char '{}' converted to index: {}", c, result);
        return result;
    }

    private static char indexToChar(int index, String supportedChars) {
        char result = (index == 0) ? '\u0001' : supportedChars.charAt(index - 1);
        log.trace("Index {} converted to char: '{}'", index, result);
        return result;
    }

    private static BigInteger toDecimal(int[] array, int radix) {
        log.trace(
                "Converting array {} to decimal with charset size {}",
                Arrays.toString(array),
                radix);
        BigInteger result = BigInteger.ZERO;
        for (int i = 0; i < array.length; i++) {
            BigInteger value = BigInteger.valueOf(array[i]);
            BigInteger multiplier = BigInteger.valueOf(radix).pow(array.length - 1 - i);
            result = result.add(value.multiply(multiplier));
        }
        log.trace("Decimal conversion result: {}", result);
        return result;
    }

    private static int[] fromDecimal(BigInteger decimal, int length, int base) {
        log.trace("Converting decimal {} to base {} array of length {}", decimal, base, length);
        int[] result = new int[length];
        BigInteger remainder = decimal;
        for (int i = length - 1; i >= 0; i--) {
            BigInteger divisor = BigInteger.valueOf(base).pow(i);
            int value = remainder.divide(divisor).intValue();
            remainder = remainder.mod(divisor);
            result[length - 1 - i] = value;
        }
        log.trace("Base conversion result: {}", Arrays.toString(result));
        return result;
    }

    private static String stringToAsciiString(
            String s,
            int expectedLength,
            boolean paddingAtEnd,
            boolean isCaseInsensitive,
            String supportedChars) {
        log.trace(
                "Converting string '{}' to ASCII string, expectedLength={}, paddingAtEnd={}, isCaseInsensitive={}",
                s,
                expectedLength,
                paddingAtEnd,
                isCaseInsensitive);
        String str = isCaseInsensitive ? s.toLowerCase() : s;
        char[] paddedChars = new char[expectedLength];

        if (paddingAtEnd) {
            for (int i = 0; i < expectedLength; i++) {
                if (i < str.length()) {
                    paddedChars[i] = str.charAt(i);
                } else {
                    paddedChars[i] = '\u0000';
                }
            }
            log.trace("Applied suffix padding to string");
        } else {
            int offset = expectedLength - str.length();
            for (int i = 0; i < expectedLength; i++) {
                if (i < offset) {
                    paddedChars[i] = '\u0000';
                } else {
                    paddedChars[i] = str.charAt(i - offset);
                }
            }
            log.trace("Applied prefix padding to string");
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < paddedChars.length; i++) {
            if (i > 0) result.append(" ");
            result.append(String.format("%03d", charToIndex(paddedChars[i], supportedChars)));
        }
        String asciiResult = result.toString();
        log.trace("ASCII string conversion result: {}", asciiResult);
        return asciiResult;
    }

    private static String convertToAsciiString(String input, String supportedChars) {
        log.trace("Converting ASCII representation '{}' back to string", input);
        String[] asciiValues = input.split(" ");
        StringBuilder result = new StringBuilder();

        for (String value : asciiValues) {
            char c = indexToChar(Integer.parseInt(value), supportedChars);
            result.append(c);
        }

        String resultString = result.toString();
        if (resultString.replaceAll("\u0001", "").isEmpty()) {
            log.trace("Detected all placeholder characters, returning empty string");
            return "";
        } else {
            log.trace("ASCII to string conversion result: '{}'", resultString);
            return resultString;
        }
    }
}

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CharsetBasedSplitterTest {

    private static final String DEFAULT_CHARSET = "0123456789abcdefghijklmnopqrstuvwxyz";

    @Test
    @DisplayName("测试最小值和最大值的编码")
    public void testMinMax() {
        String minStr = "00000";
        String maxStr = "1";
        int maxLen = Math.max(minStr.length(), maxStr.length());
        String orderedCharset = "012a34b56789";
        BigInteger minBigInt =
                CollationBasedSplitter.encodeStringToNumericRange(
                        minStr, maxLen, true, true, orderedCharset, orderedCharset.length() + 1);
        System.out.println("最小值编码: " + minBigInt);

        BigInteger maxBigInt =
                CollationBasedSplitter.encodeStringToNumericRange(
                        maxStr, maxLen, true, true, orderedCharset, orderedCharset.length() + 1);
        System.out.println("最大值编码: " + maxBigInt);

        // 确保最大值大于最小值
        assert maxBigInt.compareTo(minBigInt) > 0;
    }

    @Test
    @DisplayName("测试字符串编码和解码的一致性")
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

        // 由于填充和大小写转换，我们需要比较转换后的结果与原始字符串的小写版本（忽略填充字符）
        assertEquals(original.toLowerCase(), decoded.trim());
    }

    @Test
    @DisplayName("测试具有特殊字符的字符集")
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
    @DisplayName("测试不同填充位置的影响")
    public void testPaddingPosition() {
        String input = "xyz";
        int maxLength = 5;
        int radix = DEFAULT_CHARSET.length() + 1;

        // 前填充
        BigInteger encodedPrefix =
                CollationBasedSplitter.encodeStringToNumericRange(
                        input, maxLength, false, false, DEFAULT_CHARSET, radix);
        String decodedPrefix =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encodedPrefix.toString(), maxLength, radix, DEFAULT_CHARSET);

        // 后填充
        BigInteger encodedSuffix =
                CollationBasedSplitter.encodeStringToNumericRange(
                        input, maxLength, true, false, DEFAULT_CHARSET, radix);
        String decodedSuffix =
                CollationBasedSplitter.decodeNumericRangeToString(
                        encodedSuffix.toString(), maxLength, radix, DEFAULT_CHARSET);

        // 验证两种填充方式都能正确解码
        assertEquals(input, decodedPrefix.trim());
        assertEquals(input, decodedSuffix.trim());

        // 确保前填充和后填充产生不同的数值
        assert !encodedPrefix.equals(encodedSuffix);
    }

    @Test
    @DisplayName("测试性能")
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

        System.out.println("执行 " + iterations + " 次编码/解码操作耗时: " + duration + " 毫秒");
        System.out.println("平均每次操作耗时: " + (double) duration / iterations + " 毫秒");
    }

    @Test
    @DisplayName("测试随机字符串的编码和解码")
    public void testRandomStrings() {
        java.util.Random random = new java.util.Random();
        int testCount = 10;
        int maxLength = 20;
        int radix = DEFAULT_CHARSET.length() + 1;
        for (int test = 0; test < testCount; test++) {
            // 生成随机长度的随机字符串
            int length = random.nextInt(maxLength) + 1;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                int charIndex = random.nextInt(DEFAULT_CHARSET.length());
                sb.append(DEFAULT_CHARSET.charAt(charIndex));
            }
            String randomString = sb.toString();
            // 编码和解码
            BigInteger encoded =
                    CollationBasedSplitter.encodeStringToNumericRange(
                            randomString, maxLength, true, false, DEFAULT_CHARSET, radix);

            String decoded =
                    CollationBasedSplitter.decodeNumericRangeToString(
                            encoded.toString(), maxLength, radix, DEFAULT_CHARSET);

            System.out.println("随机字符串 #" + test + ": " + randomString);
            System.out.println("编码结果: " + encoded);
            System.out.println("解码结果: " + decoded.trim());

            assertEquals(randomString, decoded.trim());
        }
    }
}

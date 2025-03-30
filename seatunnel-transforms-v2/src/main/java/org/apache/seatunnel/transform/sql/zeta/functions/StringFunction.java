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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import org.apache.groovy.parser.antlr4.util.StringUtils;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringFunction {
    private static final byte[] SOUNDEX_INDEX =
            "71237128722455712623718272\000\000\000\000\000\00071237128722455712623718272"
                    .getBytes(StandardCharsets.ISO_8859_1);

    public static Integer ascii(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        } else {
            return (int) arg.charAt(0);
        }
    }

    public static Long bitLength(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        return arg.getBytes(StandardCharsets.UTF_8).length * 8L;
    }

    public static Long charLength(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        return (long) arg.length();
    }

    public static Long octetLength(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        return (long) arg.getBytes(StandardCharsets.UTF_8).length;
    }

    public static String chr(List<Object> args) {
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        return String.valueOf((char) ((Number) arg).intValue());
    }

    public static String concat(List<Object> args) {
        int i = 0;
        StringBuilder builder = new StringBuilder();
        for (int l = args.size(); i < l; i++) {
            Object v = args.get(i);
            if (v == null) {
                continue;
            }
            builder.append(v);
        }
        return builder.toString();
    }

    public static String concatWs(List<Object> args) {
        int i = 1;
        String separator = (String) args.get(0);
        StringBuilder builder = new StringBuilder();
        boolean f = false;
        for (int l = args.size(); i < l; i++) {
            Object arg = args.get(i);
            if (arg == null) {
                continue;
            }
            if (separator != null) {
                if (f) {
                    builder.append(separator);
                }
                f = true;
            }
            if (arg.getClass().isArray()) {
                int len = Array.getLength(arg);
                List<Object> ll = new ArrayList<>();
                for (int j = 0; j < len; j++) {
                    Object o = Array.get(arg, j);
                    ll.add(o);
                }
                String s =
                        ll.stream()
                                .filter(Objects::nonNull)
                                .map(Object::toString)
                                .collect(Collectors.joining(separator != null ? separator : ""));
                builder.append(s);
            } else {
                builder.append(arg);
            }
        }
        return builder.toString();
    }

    public static String hextoraw(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        int len = arg.length();
        if (len % 4 != 0) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Unsupported arg for function: %s", ZetaSQLFunction.HEXTORAW));
        }
        StringBuilder builder = new StringBuilder(len / 4);
        for (int i = 0; i < len; i += 4) {
            builder.append((char) Integer.parseInt(arg.substring(i, i + 4), 16));
        }
        return builder.toString();
    }

    public static String rawtohex(List<Object> args) {
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        if (arg instanceof byte[]) {
            int len = ((byte[]) arg).length;
            byte[] bytes = new byte[len * 2];
            char[] hex = "0123456789abcdef".toCharArray();
            for (int i = 0, j = 0; i < len; i++) {
                int c = ((byte[]) arg)[i] & 0xff;
                bytes[j++] = (byte) hex[c >> 4];
                bytes[j++] = (byte) hex[c & 0xf];
            }
            return new String(bytes, StandardCharsets.ISO_8859_1);
        }
        String s = arg.toString();

        int length = s.length();
        StringBuilder buff = new StringBuilder(4 * length);
        for (int i = 0; i < length; i++) {
            String hex = Integer.toHexString(s.charAt(i) & 0xffff);
            for (int j = hex.length(); j < 4; j++) {
                buff.append('0');
            }
            buff.append(hex);
        }
        return buff.toString();
    }

    public static String insert(List<Object> args) {
        String s1 = (String) args.get(0);
        int start = ((Number) args.get(1)).intValue();
        int length = ((Number) args.get(2)).intValue();
        String s2 = (String) args.get(3);
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        int len1 = s1.length();
        int len2 = s2.length();
        start--;
        if (start < 0 || length <= 0 || len2 == 0 || start > len1) {
            return s1;
        }
        if (start + length > len1) {
            length = len1 - start;
        }
        return s1.substring(0, start) + s2 + s1.substring(start + length);
    }

    public static String lower(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        return arg.toLowerCase();
    }

    public static String upper(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        return arg.toUpperCase();
    }

    public static String left(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        int count = ((Number) args.get(1)).intValue();
        if (count > arg.length()) {
            count = arg.length();
        }
        return arg.substring(0, count);
    }

    public static String right(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        int count = ((Number) args.get(1)).intValue();
        int length = arg.length();
        if (count > length) {
            count = length;
        }
        return arg.substring(length - count);
    }

    public static Integer location(String functionName, List<Object> args) {
        String search = (String) args.get(0);
        String s = (String) args.get(1);
        if (s == null) {
            return 0;
        }
        int start = 1;
        if (args.size() == 3 && functionName.equalsIgnoreCase(ZetaSQLFunction.LOCATE)) {
            start = ((Number) args.get(2)).intValue();
        }
        if (start < 0) {
            return s.lastIndexOf(search, s.length() + start) + 1;
        }
        return s.indexOf(search, start == 0 ? 0 : start - 1) + 1;
    }

    public static Integer instr(List<Object> args) {
        String s = (String) args.get(0);
        if (s == null) {
            return 0;
        }
        String search = (String) args.get(1);
        int start = 1;
        if (args.size() == 3) {
            start = ((Number) args.get(2)).intValue();
        }
        if (start < 0) {
            return s.lastIndexOf(search, s.length() + start) + 1;
        }
        return s.indexOf(search, start == 0 ? 0 : start - 1) + 1;
    }

    public static String pad(String functionName, List<Object> args) {
        String padding;
        if (args.size() >= 3) {
            padding = (String) args.get(2);
        } else {
            padding = null;
        }
        String v1 = (String) args.get(0);
        if (v1 == null) {
            return null;
        }
        int v2 = ((Number) args.get(1)).intValue();
        return pad(v1, v2, padding, functionName.equalsIgnoreCase(ZetaSQLFunction.RPAD));
    }

    public static String pad(String string, int n, String padding, boolean right) {
        if (n < 0) {
            n = 0;
        }
        if (n < string.length()) {
            return string.substring(0, n);
        } else if (n == string.length()) {
            return string;
        }
        char paddingChar;
        if (padding == null || padding.isEmpty()) {
            paddingChar = ' ';
        } else {
            paddingChar = padding.charAt(0);
        }
        StringBuilder buff = new StringBuilder(n);
        n -= string.length();
        if (right) {
            buff.append(string);
        }
        for (int i = 0; i < n; i++) {
            buff.append(paddingChar);
        }
        if (!right) {
            buff.append(string);
        }
        return buff.toString();
    }

    public static String ltrim(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        String sp = null;
        if (args.size() >= 2) {
            sp = (String) args.get(1);
        }
        return trim(arg, true, false, sp);
    }

    public static String rtrim(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        String sp = null;
        if (args.size() >= 2) {
            sp = (String) args.get(1);
        }
        return trim(arg, false, true, sp);
    }

    public static String trim(List<Object> args) {
        String arg = (String) args.get(0);
        if (arg == null) {
            return null;
        }
        String sp = null;
        if (args.size() >= 2) {
            sp = (String) args.get(1);
        }
        return trim(arg, true, true, sp);
    }

    public static String[] split(List<Object> args) {
        String arg = (String) args.get(0);
        if (StringUtils.isEmpty(arg)) {
            return null;
        }
        String delimiter = "";
        if (args.size() >= 2) {
            delimiter = (String) args.get(1);
        }
        return arg.split(delimiter);
    }

    public static String trim(String s, boolean leading, boolean trailing, String sp) {
        char space = sp == null || sp.isEmpty() ? ' ' : sp.charAt(0);
        int begin = 0, end = s.length();
        if (leading) {
            while (begin < end && s.charAt(begin) == space) {
                begin++;
            }
        }
        if (trailing) {
            while (end > begin && s.charAt(end - 1) == space) {
                end--;
            }
        }
        // substring() returns self if start == 0 && end == length()
        return s.substring(begin, end);
    }

    public static String regexpReplace(List<Object> args) {
        String input = (String) args.get(0);
        if (input == null) {
            return null;
        }
        String regexp = (String) args.get(1);
        String replacement = (String) args.get(2);
        String regexpMode = null;
        if (args.size() >= 4) {
            regexpMode = (String) args.get(3);
        }
        return regexpReplace(input, regexp, replacement, 1, 0, regexpMode);
    }

    private static String regexpReplace(
            String input,
            String regexp,
            String replacement,
            int position,
            int occurrence,
            String regexpMode) {
        int flags = makeRegexpFlags(regexpMode, false);
        Matcher matcher =
                Pattern.compile(regexp, flags).matcher(input).region(position - 1, input.length());
        if (occurrence == 0) {
            return matcher.replaceAll(replacement);
        } else {
            StringBuffer sb = new StringBuffer();
            int index = 1;
            while (matcher.find()) {
                if (index == occurrence) {
                    matcher.appendReplacement(sb, replacement);
                    break;
                }
                index++;
            }
            matcher.appendTail(sb);
            return sb.toString();
        }
    }

    public static Boolean regexpLike(List<Object> args) {
        String input = (String) args.get(0);
        if (input == null) {
            return null;
        }
        String regexp = (String) args.get(1);
        String regexpMode = null;
        if (args.size() >= 3) {
            regexpMode = (String) args.get(2);
        }
        int flags = makeRegexpFlags(regexpMode, false);
        return Pattern.compile(regexp, flags).matcher(input).find();
    }

    private static int makeRegexpFlags(String stringFlags, boolean ignoreGlobalFlag) {
        int flags = Pattern.UNICODE_CASE;
        if (stringFlags != null) {
            for (int i = 0; i < stringFlags.length(); ++i) {
                switch (stringFlags.charAt(i)) {
                    case 'i':
                        flags |= Pattern.CASE_INSENSITIVE;
                        break;
                    case 'c':
                        flags &= ~Pattern.CASE_INSENSITIVE;
                        break;
                    case 'n':
                        flags |= Pattern.DOTALL;
                        break;
                    case 'm':
                        flags |= Pattern.MULTILINE;
                        break;
                    case 'g':
                        if (ignoreGlobalFlag) {
                            break;
                        }
                        // $FALL-THROUGH$
                    default:
                        throw new TransformException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                                String.format(
                                        "Unsupported regexpMode arg: %s for function: %s",
                                        flags, ZetaSQLFunction.HEXTORAW));
                }
            }
        }
        return flags;
    }

    public static String regexpSubstr(List<Object> args) {
        String input = (String) args.get(0);
        if (input == null) {
            return null;
        }
        String regexp = (String) args.get(1);
        if (args.size() == 2) {
            return regexpSubstr(input, regexp, null, null, null, null);
        }
        if (args.size() >= 6) {
            Integer positionArg = null;
            if (args.get(2) != null) {
                positionArg = ((Number) args.get(2)).intValue();
            }
            Integer occurrenceArg = null;
            if (args.get(3) != null) {
                occurrenceArg = ((Number) args.get(3)).intValue();
            }
            String regexpMode = (String) args.get(4);
            Integer subexpressionArg = null;
            if (args.get(5) != null) {
                subexpressionArg = ((Number) args.get(5)).intValue();
            }
            return regexpSubstr(
                    input, regexp, positionArg, occurrenceArg, regexpMode, subexpressionArg);
        }

        return null;
    }

    public static String regexpSubstr(
            String input,
            String regexp,
            Integer positionArg,
            Integer occurrenceArg,
            String regexpMode,
            Integer subexpressionArg) {
        int position = positionArg != null ? positionArg - 1 : 0;
        int requestedOccurrence = occurrenceArg != null ? occurrenceArg : 1;
        int subexpression = subexpressionArg != null ? subexpressionArg : 0;
        int flags = makeRegexpFlags(regexpMode, false);
        Matcher m = Pattern.compile(regexp, flags).matcher(input);

        boolean found = m.find(position);
        for (int occurrence = 1; occurrence < requestedOccurrence && found; occurrence++) {
            found = m.find();
        }

        if (!found) {
            return null;
        } else {
            return m.group(subexpression);
        }
    }

    public static String repeat(List<Object> args) {
        String s = (String) args.get(0);
        if (s == null) {
            return null;
        }
        int count = ((Number) args.get(1)).intValue();
        if (count <= 0) {
            return "";
        }
        int length = s.length();
        StringBuilder builder = new StringBuilder(length * count);
        while (count-- > 0) {
            builder.append(s);
        }
        return builder.toString();
    }

    public static String replace(List<Object> args) {
        String v1 = (String) args.get(0);
        if (v1 == null) {
            return null;
        }
        String v2 = (String) args.get(1);
        String after;
        if (args.size() >= 3) {
            after = (String) args.get(2);
            if (after == null) {
                after = "";
            }
        } else {
            after = "";
        }
        return replaceAll(v1, v2, after);
    }

    public static String replaceAll(String s, String before, String after) {
        int next = s.indexOf(before);
        if (next < 0 || before.isEmpty()) {
            return s;
        }
        StringBuilder buff = new StringBuilder(s.length() - before.length() + after.length());
        int index = 0;
        while (true) {
            buff.append(s, index, next).append(after);
            index = next + before.length();
            next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s, index, s.length());
                break;
            }
        }
        return buff.toString();
    }

    public static String soundex(List<Object> args) {
        String v1 = (String) args.get(0);
        if (v1 == null) {
            return null;
        }
        return new String(getSoundex(v1), StandardCharsets.ISO_8859_1);
    }

    private static byte[] getSoundex(String s) {
        byte[] chars = {'0', '0', '0', '0'};
        byte lastDigit = '0';
        for (int i = 0, j = 0, l = s.length(); i < l && j < 4; i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'z') {
                byte newDigit = SOUNDEX_INDEX[c - 'A'];
                if (newDigit != 0) {
                    if (j == 0) {
                        chars[j++] = (byte) (c & 0xdf); // Converts a-z to A-Z
                        lastDigit = newDigit;
                    } else if (newDigit <= '6') {
                        if (newDigit != lastDigit) {
                            chars[j++] = lastDigit = newDigit;
                        }
                    } else if (newDigit == '7') {
                        lastDigit = newDigit;
                    }
                }
            }
        }
        return chars;
    }

    public static String space(List<Object> args) {
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }

        byte[] chars = new byte[Math.max(0, ((Number) arg).intValue())];
        Arrays.fill(chars, (byte) ' ');
        return new String(chars, StandardCharsets.ISO_8859_1);
    }

    public static String substring(List<Object> args) {
        String s = (String) args.get(0);
        if (s == null) {
            return null;
        }
        int sl = s.length();
        int start = ((Number) args.get(1)).intValue();
        Object v3 = null;
        if (args.size() >= 3) {
            v3 = args.get(2);
        }
        // These compatibility conditions violate the Standard
        if (start == 0) {
            start = 1;
        } else if (start < 0) {
            start = sl + start + 1;
        }
        int end = v3 == null ? Math.max(sl + 1, start) : start + ((Number) v3).intValue();
        // SQL Standard requires "data exception - substring error" when
        // end < start but H2 does not throw it for compatibility
        start = Math.max(start, 1);
        end = Math.min(end, sl + 1);
        if (start > sl || end <= start) {
            return null;
        }
        return s.substring(start - 1, end - 1);
    }

    public static String toChar(List<Object> args) {
        Object arg = args.get(0);
        if (arg == null) {
            return null;
        }
        if (arg instanceof Number) {
            return arg.toString();
        }
        if (arg instanceof Temporal) {
            return DateTimeFunction.formatdatetime(args);
        }
        return arg.toString();
    }

    public static String translate(List<Object> args) {
        String original = (String) args.get(0);
        if (original == null) {
            return null;
        }
        String findChars = (String) args.get(1);
        String replaceChars = (String) args.get(2);
        // if it stays null, then no replacements have been made
        StringBuilder builder = null;
        // if shorter than findChars, then characters are removed
        // (if null, we don't access replaceChars at all)
        int replaceSize = replaceChars == null ? 0 : replaceChars.length();
        for (int i = 0, size = original.length(); i < size; i++) {
            char ch = original.charAt(i);
            int index = findChars.indexOf(ch);
            if (index >= 0) {
                if (builder == null) {
                    builder = new StringBuilder(size);
                    if (i > 0) {
                        builder.append(original, 0, i);
                    }
                }
                if (index < replaceSize) {
                    ch = replaceChars.charAt(index);
                }
            }
            if (builder != null) {
                builder.append(ch);
            }
        }
        return builder == null ? original : builder.toString();
    }
}

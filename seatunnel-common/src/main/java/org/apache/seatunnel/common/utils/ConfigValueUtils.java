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

package org.apache.seatunnel.common.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConfigValueUtils {

    private static final Set<Character> START_DELIMITERS =
            new HashSet<>(Arrays.asList('=', ':', '{', '[', ','));
    private static final Set<Character> END_DELIMITERS =
            new HashSet<>(Arrays.asList(',', '}', ']', ':'));

    private ConfigValueUtils() {}

    public static boolean isEscapedQuote(String value, int quoteIndex) {
        int backslashCount = 0;
        int i = quoteIndex - 1;
        while (i >= 0 && value.charAt(i) == '\\') {
            backslashCount++;
            i--;
        }
        return backslashCount % 2 == 1;
    }

    public static boolean updateQuoteState(String value, int quoteIndex, boolean insideQuotes) {

        boolean isStartWrapper = isStartWrapper(insideQuotes, value, quoteIndex);
        boolean isEndWrapper = isEndWrapper(insideQuotes, value, quoteIndex);

        if (isStartWrapper) {
            insideQuotes = true;
        } else if (isEndWrapper) {
            insideQuotes = false;
        } else {
            insideQuotes = !insideQuotes;
        }
        return insideQuotes;
    }

    private static boolean isStartWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char prev = (quoteIndex > 0) ? value.charAt(quoteIndex - 1) : 0;
        char beforePrev = (quoteIndex > 1) ? value.charAt(quoteIndex - 2) : 0;

        return !insideQuotes
                && (quoteIndex == 0
                        || START_DELIMITERS.contains(prev)
                        || (prev == ' ' && START_DELIMITERS.contains(beforePrev)));
    }

    private static boolean isEndWrapper(boolean insideQuotes, String value, int quoteIndex) {
        char next = (quoteIndex + 1 < value.length()) ? value.charAt(quoteIndex + 1) : 0;
        char afterNext = (quoteIndex + 2 < value.length()) ? value.charAt(quoteIndex + 2) : 0;

        return insideQuotes
                && (quoteIndex == value.length() - 1
                        || END_DELIMITERS.contains(next)
                        || (next == ' ' && END_DELIMITERS.contains(afterNext)));
    }
}

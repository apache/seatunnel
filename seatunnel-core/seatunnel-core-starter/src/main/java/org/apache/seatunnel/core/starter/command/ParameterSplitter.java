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
package org.apache.seatunnel.core.starter.command;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ParameterSplitter implements IParameterSplitter {

    private static final Set<Character> START_DELIMITERS =
            new HashSet<>(Arrays.asList('=', ':', '{', '[', ','));
    private static final Set<Character> END_DELIMITERS =
            new HashSet<>(Arrays.asList(',', '}', ']', ':'));

    @Override
    public List<String> split(String value) {
        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideQuotes = false;
        int braceDepth = 0;
        int bracketDepth = 0;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == '"') {
                if (isEscapedQuote(value, i)) {
                    currentToken.append(c);
                    continue;
                }
                char prev = (i > 0) ? value.charAt(i - 1) : 0;
                char beforePrev = (i > 1) ? value.charAt(i - 2) : 0;
                char next = (i + 1 < value.length()) ? value.charAt(i + 1) : 0;
                char afterNext = (i + 2 < value.length()) ? value.charAt(i + 2) : 0;

                boolean isStartWrapper =
                        !insideQuotes
                                && (i == 0
                                        || START_DELIMITERS.contains(prev)
                                        || (prev == ' ' && START_DELIMITERS.contains(beforePrev)));

                boolean isEndWrapper =
                        insideQuotes
                                && (i == value.length() - 1
                                        || END_DELIMITERS.contains(next)
                                        || (next == ' ' && END_DELIMITERS.contains(afterNext)));

                if (isStartWrapper) {
                    insideQuotes = true;
                } else if (isEndWrapper) {
                    insideQuotes = false;
                }
                currentToken.append(c);
                continue;
            }

            if (!insideQuotes) {
                if (c == '{') {
                    braceDepth++;
                } else if (c == '}' && braceDepth > 0) {
                    braceDepth--;
                } else if (c == '[') {
                    bracketDepth++;
                } else if (c == ']' && bracketDepth > 0) {
                    bracketDepth--;
                }
            }

            if (c == ',' && !insideQuotes && braceDepth == 0 && bracketDepth == 0) {
                result.add(currentToken.toString().trim());
                currentToken = new StringBuilder();
            } else {
                currentToken.append(c);
            }
        }

        if (currentToken.length() > 0) {
            result.add(currentToken.toString().trim());
        }

        return result;
    }

    private boolean isEscapedQuote(String value, int quoteIndex) {
        int backslashCount = 0;
        int i = quoteIndex - 1;
        while (i >= 0 && value.charAt(i) == '\\') {
            backslashCount++;
            i--;
        }
        return backslashCount % 2 == 1;
    }
}

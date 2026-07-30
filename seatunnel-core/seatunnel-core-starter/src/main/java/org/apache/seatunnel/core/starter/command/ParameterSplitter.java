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
import java.util.List;

public class ParameterSplitter implements IParameterSplitter {

    @Override
    public List<String> split(String value) {

        List<String> result = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideQuotes = false;
        int braceDepth = 0;
        int bracketDepth = 0;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == '\\' && i + 1 < value.length()) {
                char next = value.charAt(i + 1);
                currentToken.append(next);
                i++;
                continue;
            }
            if (c == '"') {
                insideQuotes = !insideQuotes;
            } else if (!insideQuotes) {
                if (c == '{') {
                    braceDepth++;
                } else if (c == '}') {
                    braceDepth--;
                } else if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
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

        if (braceDepth != 0 || bracketDepth != 0 || insideQuotes) {
            throw new IllegalArgumentException(
                    "Invalid parameter string: unmatched braces/brackets or unclosed quotes");
        }

        return result;
    }
}

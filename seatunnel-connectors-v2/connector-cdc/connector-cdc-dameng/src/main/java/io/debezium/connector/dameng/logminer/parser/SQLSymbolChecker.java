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

package io.debezium.connector.dameng.logminer.parser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SQLSymbolChecker {
    public static final SQLSymbolChecker NUMBER_CHECKER = new SQLSymbolChecker(
        '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9');
    public static final SQLSymbolChecker SKIP_CHECKER = new SQLSymbolChecker(
        '\t', '\n', '\r', ' ');
    public static final SQLSymbolChecker NAMING_CHECKER = new SQLSymbolChecker(
        '_', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');

    private final Set<Character> symbols;

    public SQLSymbolChecker(Character... symbols) {
        this.symbols = new HashSet<>(Arrays.asList(symbols));
    }

    public boolean contains(char c) {
        return symbols.contains(c);
    }
}

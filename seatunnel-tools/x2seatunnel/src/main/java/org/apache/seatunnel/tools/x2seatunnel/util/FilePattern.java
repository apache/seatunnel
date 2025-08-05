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

package org.apache.seatunnel.tools.x2seatunnel.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FilePattern {

    /**
     * Filters the file list according to the wildcard patterns separated by commas.
     *
     * @param files The list of all file paths.
     * @param patterns The wildcard patterns, such as "*.json,*.xml".
     * @return The list of files that match the patterns.
     */
    public static List<String> filter(List<String> files, String patterns) {
        if (patterns == null || patterns.trim().isEmpty()) {
            return files;
        }
        String[] pats = patterns.split(",");
        List<Pattern> regexList = new ArrayList<>();
        for (String p : pats) {
            String pat = p.trim().replace(".", "\\.").replace("*", ".*");
            regexList.add(Pattern.compile(pat));
        }
        return files.stream()
                .filter(f -> regexList.stream().anyMatch(r -> r.matcher(f).matches()))
                .collect(Collectors.toList());
    }
}

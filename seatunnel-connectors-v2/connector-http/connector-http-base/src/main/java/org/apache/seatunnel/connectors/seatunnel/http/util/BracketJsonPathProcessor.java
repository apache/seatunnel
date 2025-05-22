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

package org.apache.seatunnel.connectors.seatunnel.http.util;

import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Processor for handling JsonPath with bracket notation (e.g., $['result']['value']). */
public class BracketJsonPathProcessor extends AbstractJsonPathProcessor {

    private static final Pattern BRACKET_PATTERN =
            Pattern.compile("\\['([^']*)'\\]|\\[\"([^\"]*)\"\\]");

    /** {@inheritDoc} */
    @Override
    public boolean canProcess(String pathString) {
        return pathString.contains("['") || pathString.contains("[\"");
    }

    /** {@inheritDoc} */
    @Override
    public String extractCommonParentPath(JsonPath[] paths) {
        if (paths == null || paths.length == 0) {
            return null;
        }

        // Get all paths as strings
        String[] pathStrings = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            pathStrings[i] = paths[i].getPath();
        }

        // For bracket notation, we'll look for common segments based on ['xxx'] patterns
        StringBuilder commonPrefix = new StringBuilder("$");
        List<List<String>> allSegments = new ArrayList<>();

        // Extract segments from each path
        for (String pathString : pathStrings) {
            Matcher matcher = BRACKET_PATTERN.matcher(pathString);
            List<String> segments = new ArrayList<>();

            while (matcher.find()) {
                // Group 1 is for single quotes, group 2 is for double quotes
                String segment = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
                segments.add(segment);
            }

            allSegments.add(segments);
        }

        // Find the minimum length of segments
        int minLength = Integer.MAX_VALUE;
        for (List<String> segments : allSegments) {
            minLength = Math.min(minLength, segments.size());
        }

        // No segments in common beyond $
        if (minLength == 0) {
            return "$";
        }

        // Find common segments
        for (int i = 0; i < minLength; i++) {
            String segment = allSegments.get(0).get(i);
            boolean allMatch = true;

            for (int j = 1; j < allSegments.size(); j++) {
                if (!segment.equals(allSegments.get(j).get(i))) {
                    allMatch = false;
                    break;
                }
            }

            if (!allMatch) {
                break;
            }

            commonPrefix.append("['").append(segment).append("']");
        }

        return commonPrefix.toString();
    }

    /** {@inheritDoc} */
    @Override
    public String getRelativePath(String parentPath, String fullPath) {
        if (parentPath.equals("$")) {
            return "$" + fullPath.substring(1);
        }

        // Ensure the parent path is contained in the full path
        if (!fullPath.startsWith(parentPath)) {
            throw new IllegalArgumentException("Full path must start with parent path");
        }

        String relativePath = fullPath.substring(parentPath.length());

        // Convert the relative path to use dot notation for simpler processing
        return "$" + relativePath;
    }

    /** {@inheritDoc} */
    @Override
    public List<Map<String, Object>> readObjectsFromPath(ReadContext jsonReadContext, String path) {
        try {
            Object result = jsonReadContext.read(path);
            return wrapResult(result);
        } catch (Exception e) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                    String.format(
                            "Failed to read data from JSON using path %s: %s",
                            path, e.getMessage()));
        }
    }

    /**
     * Wraps a result object in a standardized format.
     *
     * @param result The result object to wrap
     * @return A list of maps containing the result
     */
    private List<Map<String, Object>> wrapResult(Object result) {
        List<Map<String, Object>> objects = new ArrayList<>();

        if (result instanceof Map) {
            objects.add((Map<String, Object>) result);
        } else if (result instanceof List) {
            List<?> list = (List<?>) result;
            if (list.isEmpty()) {
                return Collections.emptyList();
            }

            if (list.get(0) instanceof Map) {
                for (Object item : list) {
                    objects.add((Map<String, Object>) item);
                }
            } else {
                objects.add(Collections.singletonMap("result", list));
            }
        } else {
            objects.add(Collections.singletonMap("result", result));
        }

        return objects;
    }
}

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
import java.util.List;
import java.util.Map;

/** Processor for handling JsonPath with array notation (using [*]). */
public class ArrayJsonPathProcessor extends JsonPathProcessorImpl {
    /**
     * Extract the common parent path from an array of JsonPaths.
     *
     * @param paths Array of JsonPath objects
     * @return The common parent path as a string
     */
    private String extractCommonParentPath(JsonPath[] paths) {
        if (paths == null || paths.length == 0) {
            return null;
        }

        // Get all paths as strings
        String[] pathStrings = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            pathStrings[i] = paths[i].getPath();
        }

        String firstPath = pathStrings[0];
        int arrayPos = firstPath.indexOf("[*]");

        if (arrayPos == -1) {
            return null; // Not an array path, cannot process
        }

        String parentPath = firstPath.substring(0, arrayPos + 3);

        // Verify all other paths have the same parent
        for (int i = 1; i < pathStrings.length; i++) {
            if (!pathStrings[i].startsWith(parentPath)) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                        String.format(
                                "Paths have different array parents. Expected '%s' but found path starting with '%s'",
                                parentPath, pathStrings[i]));
            }
        }

        return parentPath;
    }

    /**
     * Get a relative path based on a parent path and a full path.
     *
     * @param parentPath The parent path
     * @param fullPath The complete path
     * @return The relative path from parent to full path
     */
    private String getRelativePath(String parentPath, String fullPath) {
        if (!parentPath.contains("[*]")) {
            throw new IllegalArgumentException(
                    "Parent path must contain [*] for ArrayJsonPathProcessor");
        }

        if (!fullPath.contains("[*]")) {
            // For non-array paths when parent has [*], extract the correct relative part
            String commonPart = parentPath.substring(0, parentPath.indexOf("[*]"));
            String relativePart = fullPath.substring(commonPart.length());

            // If the relative part starts with a dot, remove it
            if (relativePart.startsWith(".")) {
                relativePart = relativePart.substring(1);
            }

            return "$." + relativePart;
        } else {
            // Original implementation for array paths
            String relativePart = fullPath.substring(parentPath.length());

            // If the relative part starts with a dot, remove it
            if (relativePart.startsWith(".")) {
                relativePart = relativePart.substring(1);
            }

            return "$." + relativePart;
        }
    }

    /**
     * Read objects from a specific path in JSON.
     *
     * @param jsonReadContext The JSON read context
     * @param path The path to read from
     * @return List of objects read from the path
     */
    private List<Map<String, Object>> readObjectsFromPath(
            ReadContext jsonReadContext, String path) {
        try {
            return jsonReadContext.read(path);
        } catch (Exception e) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                    String.format(
                            "Failed to read data from JSON using path %s: %s",
                            path, e.getMessage()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<List<String>> processJsonData(ReadContext jsonReadContext, JsonPath[] paths) {
        String commonParentPath = extractCommonParentPath(paths);
        if (commonParentPath == null) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                    "Could not find common parent path in JsonPaths. All paths must share a common array parent.");
        }

        List<Map<String, Object>> objects = readObjectsFromPath(jsonReadContext, commonParentPath);

        // If we're allowing null values for missing fields, we don't need additional validation
        return processObjects(objects, commonParentPath, paths);
    }

    /**
     * Process objects extracted from JSON and convert them to the result format.
     *
     * @param objects List of objects extracted from JSON
     * @param commonParentPath The common parent path used for extraction
     * @param paths Array of JsonPath objects
     * @return List of processed data
     */
    private List<List<String>> processObjects(
            List<Map<String, Object>> objects, String commonParentPath, JsonPath[] paths) {
        List<List<String>> results = initializeResults(paths.length, objects.size());

        for (int objIndex = 0; objIndex < objects.size(); objIndex++) {
            Map<String, Object> obj = objects.get(objIndex);
            ReadContext objContext = JsonPath.parse(obj);

            for (int pathIndex = 0; pathIndex < paths.length; pathIndex++) {
                String fieldPath = paths[pathIndex].getPath();
                String relativePath = getRelativePath(commonParentPath, fieldPath);
                String value = extractValue(objContext, relativePath);
                results.get(pathIndex).add(value);
            }
        }

        return dataFlip(results);
    }

    /**
     * Initialize a results list with the given dimensions.
     *
     * @param pathCount Number of paths (rows)
     * @param objectCount Number of objects (columns)
     * @return Initialized results list
     */
    private List<List<String>> initializeResults(int pathCount, int objectCount) {
        List<List<String>> results = new ArrayList<>(pathCount);
        for (int i = 0; i < pathCount; i++) {
            List<String> row = new ArrayList<>(objectCount);
            results.add(row);
        }
        return results;
    }
}

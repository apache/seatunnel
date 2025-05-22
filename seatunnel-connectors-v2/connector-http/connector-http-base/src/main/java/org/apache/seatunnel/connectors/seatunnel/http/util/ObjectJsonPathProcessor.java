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

/** Processor for handling JsonPath with dot notation (standard object notation). */
public class ObjectJsonPathProcessor extends AbstractJsonPathProcessor {

    /** {@inheritDoc} */
    @Override
    public boolean canProcess(String pathString) {
        // Check if path uses dot notation and doesn't use array notation or bracket notation
        return pathString.contains(".")
                && !pathString.contains("[*]")
                && !pathString.contains("['")
                && !pathString.contains("[\"");
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

        // Handle dot notation paths (ensure we return at least $)
        String[] components = pathStrings[0].split("\\.");
        StringBuilder prefix = new StringBuilder();
        boolean allMatch = true;
        boolean addedAnyComponent = false;

        for (int i = 0; i < components.length; i++) {
            String currentComponent = components[i];

            for (int j = 1; j < pathStrings.length; j++) {
                String[] otherComponents = pathStrings[j].split("\\.");
                if (i >= otherComponents.length || !otherComponents[i].equals(currentComponent)) {
                    allMatch = false;
                    break;
                }
            }

            if (!allMatch) {
                break;
            }

            if (prefix.length() > 0) {
                prefix.append(".");
            }
            prefix.append(currentComponent);
            addedAnyComponent = true;
        }

        // If no common components were found, return $ as the minimum common prefix
        return addedAnyComponent ? prefix.toString() : "$";
    }

    /** {@inheritDoc} */
    @Override
    public String getRelativePath(String parentPath, String fullPath) {
        // Calculate relative path for object structures
        String relativePart;

        // If parentPath is just "$", take everything after "$"
        if (parentPath.equals("$")) {
            relativePart = fullPath.substring(1);
            // If path starts with a dot, remove it
            if (relativePart.startsWith(".")) {
                relativePart = relativePart.substring(1);
            }
            return "$." + relativePart;
        }

        relativePart = fullPath.substring(parentPath.length());

        // If the relative part starts with a dot, remove it
        if (relativePart.startsWith(".")) {
            relativePart = relativePart.substring(1);
        }

        return "$." + relativePart;
    }

    /** {@inheritDoc} */
    @Override
    public List<Map<String, Object>> readObjectsFromPath(ReadContext jsonReadContext, String path) {
        try {
            Object result = jsonReadContext.read(path);
            return wrapSingleObject(result);
        } catch (Exception e) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                    String.format(
                            "Failed to read data from JSON using path %s: %s",
                            path, e.getMessage()));
        }
    }

    /**
     * Wraps a single object or list in a standard format.
     *
     * @param result The object to wrap
     * @return A list of maps containing the object(s)
     */
    private List<Map<String, Object>> wrapSingleObject(Object result) {
        List<Map<String, Object>> objects = new ArrayList<>();
        if (result instanceof Map) {
            objects.add((Map<String, Object>) result);
        } else if (result instanceof List) {
            List<Object> resultList = (List<Object>) result;
            if (resultList.size() == 1 && resultList.get(0) instanceof Map) {
                objects.add((Map<String, Object>) resultList.get(0));
            } else {
                objects.add(Collections.singletonMap("result", result));
            }
        } else {
            objects.add(Collections.singletonMap("result", result));
        }
        return objects;
    }
}

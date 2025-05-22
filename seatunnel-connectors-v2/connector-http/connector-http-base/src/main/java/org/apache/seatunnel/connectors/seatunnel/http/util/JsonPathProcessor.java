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

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.List;
import java.util.Map;

/**
 * Interface for processing JsonPath operations. Different implementations can handle various
 * JsonPath formats.
 */
public interface JsonPathProcessor {

    /**
     * Extract the common parent path from an array of JsonPaths.
     *
     * @param paths Array of JsonPath objects
     * @return The common parent path as a string
     */
    String extractCommonParentPath(JsonPath[] paths);

    /**
     * Get a relative path based on a parent path and a full path.
     *
     * @param parentPath The parent path
     * @param fullPath The complete path
     * @return The relative path from parent to full path
     */
    String getRelativePath(String parentPath, String fullPath);

    /**
     * Check if this processor can handle the given JsonPath format.
     *
     * @param pathString The JsonPath string to check
     * @return true if this processor can handle the path, false otherwise
     */
    boolean canProcess(String pathString);

    /**
     * Process objects from a JSON structure based on JsonPaths.
     *
     * @param jsonReadContext The JSON read context
     * @param paths Array of JsonPath objects
     * @return List of extracted data
     */
    List<List<String>> processJsonData(ReadContext jsonReadContext, JsonPath[] paths);

    /**
     * Read objects from a specific path in JSON.
     *
     * @param jsonReadContext The JSON read context
     * @param path The path to read from
     * @return List of objects read from the path
     */
    List<Map<String, Object>> readObjectsFromPath(ReadContext jsonReadContext, String path);

    /**
     * Extract value from a JSON context using a relative path.
     *
     * @param objContext The JSON read context
     * @param relativePath The relative path to extract from
     * @return The extracted value as a string
     */
    String extractValue(ReadContext objContext, String relativePath);
}

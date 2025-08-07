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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;

import java.util.ArrayList;
import java.util.List;

/** Default implementation of JsonPathProcessor providing common functionality. */
public class JsonPathProcessorImpl implements JsonPathProcessor {

    /** Flag to indicate whether to return null for missing fields */
    private boolean jsonFiledMissedReturnNull = false;

    /**
     * Set whether to return null for missing fields.
     *
     * @param jsonFiledMissedReturnNull true to return null for missing fields, false otherwise
     */
    public void setJsonFiledMissedReturnNull(boolean jsonFiledMissedReturnNull) {
        this.jsonFiledMissedReturnNull = jsonFiledMissedReturnNull;
    }

    /**
     * Check if json fields with missing values should return null. This is used to determine
     * whether to validate result consistency.
     *
     * @return true if missing fields should return null, false otherwise
     */
    protected boolean isJsonFiledMissedReturnNull() {
        return jsonFiledMissedReturnNull;
    }

    /** {@inheritDoc} */
    @Override
    public List<List<String>> processJsonData(ReadContext jsonReadContext, JsonPath[] paths) {
        // Default implementation - can be overridden by subclasses
        List<List<String>> results = new ArrayList<>(paths.length);

        // Read all paths
        for (JsonPath path : paths) {
            results.add(jsonReadContext.read(path));
        }

        // Only validate consistency if jsonFiledMissedReturnNull is false
        boolean shouldValidate = !isJsonFiledMissedReturnNull();
        if (shouldValidate) {
            validateResultsConsistency(results, paths);
        }

        return dataFlip(results);
    }

    /**
     * Helper method to validate that all results have the same size.
     *
     * @param results The list of results to validate
     * @param paths The JsonPath objects used to generate the results
     * @throws HttpConnectorException if results are inconsistent
     */
    protected void validateResultsConsistency(List<List<String>> results, JsonPath[] paths) {
        if (results.isEmpty()) {
            return;
        }

        int expectedSize = results.get(0).size();
        for (int i = 1; i < results.size(); i++) {
            if (results.get(i).size() != expectedSize) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                        String.format(
                                "[%s](%d) and [%s](%d) the number of parsing records is inconsistent.",
                                paths[0].getPath(),
                                expectedSize,
                                paths[i].getPath(),
                                results.get(i).size()));
            }
        }
    }

    /**
     * Flips a matrix of results so that rows become columns and vice versa.
     *
     * @param results The original data matrix
     * @return The flipped data matrix
     */
    protected List<List<String>> dataFlip(List<List<String>> results) {
        List<List<String>> datas = new ArrayList<>();

        for (int i = 0; i < results.size(); i++) {
            List<String> result = results.get(i);
            if (i == 0) {
                for (Object o : result) {
                    String val = o == null ? null : o.toString();
                    List<String> row = new ArrayList<>(results.size());
                    row.add(val);
                    datas.add(row);
                }
            } else {
                for (int j = 0; j < result.size(); j++) {
                    Object o = result.get(j);
                    String val = o == null ? null : o.toString();
                    List<String> row = datas.get(j);
                    row.add(val);
                }
            }
        }

        return datas;
    }

    /**
     * Extract value from a JSON context using a relative path.
     *
     * @param objContext The JSON read context
     * @param relativePath The relative path to extract from
     * @return The extracted value as a string
     */
    protected String extractValue(ReadContext objContext, String relativePath) {
        try {
            Object value = objContext.read(relativePath);
            if (value == null) {
                return null;
            }
            if (value instanceof String) {
                // For string types, return the original value directly without JSON serialization,
                // otherwise "value" will become "\"value"\"
                return (String) value;
            }
            if (value instanceof List) {
                List<?> list = (List<?>) value;
                return !list.isEmpty() ? JsonUtils.toJsonString(list) : null;
            }
            // For other non-string values, use JsonUtils to serialize them.
            return JsonUtils.toJsonString(value);
        } catch (Exception e) {
            return null;
        }
    }
}

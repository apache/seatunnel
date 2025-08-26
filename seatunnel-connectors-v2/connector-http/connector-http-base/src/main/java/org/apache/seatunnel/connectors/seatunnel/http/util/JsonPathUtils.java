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

import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class for JsonPath operations. */
public class JsonPathUtils {

    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };

    private static final Configuration JSON_CONFIGURATION =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);

    /**
     * Creates a ReadContext from a JSON string.
     *
     * @param json The JSON string
     * @return A ReadContext for the JSON
     */
    public static ReadContext parseJson(String json) {
        return JsonPath.using(JSON_CONFIGURATION).parse(json);
    }

    /**
     * Creates JsonPath array from JsonField.
     *
     * @param jsonField The JsonField to convert
     * @return Array of JsonPath objects
     */
    public static JsonPath[] createJsonPaths(JsonField jsonField) {
        if (jsonField == null || jsonField.getFields() == null || jsonField.getFields().isEmpty()) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                    "JsonField cannot be null or empty");
        }

        JsonPath[] jsonPaths = new JsonPath[jsonField.getFields().size()];
        int index = 0;
        for (String pathString : jsonField.getFields().values()) {
            jsonPaths[index++] = JsonPath.compile(pathString);
        }

        return jsonPaths;
    }

    /**
     * Converts parsed data to a list of maps.
     *
     * @param data The raw data (list of lists)
     * @param jsonField The JsonField containing field names
     * @return List of maps with field names as keys
     */
    public static List<Map<String, String>> parseToMap(
            List<List<String>> data, JsonField jsonField) {
        List<Map<String, String>> resultList = new ArrayList<>(data.size());
        String[] keys = jsonField.getFields().keySet().toArray(new String[0]);

        for (List<String> row : data) {
            Map<String, String> resultMap = new HashMap<>(jsonField.getFields().size());
            for (int i = 0; i < row.size(); i++) {
                resultMap.put(keys[i], row.get(i));
            }
            resultList.add(resultMap);
        }

        return resultList;
    }
}

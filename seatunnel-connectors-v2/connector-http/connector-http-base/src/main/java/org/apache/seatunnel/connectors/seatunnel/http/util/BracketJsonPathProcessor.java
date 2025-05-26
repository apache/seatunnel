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

import java.util.ArrayList;
import java.util.List;

/** Processor for handling JsonPath with bracket notation (e.g., $['result']['value']). */
public class BracketJsonPathProcessor extends AbstractJsonPathProcessor {
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
}

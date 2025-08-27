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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Factory for creating appropriate JsonPathProcessor instances based on the JsonPath format. */
public class JsonPathProcessorFactory {

    // List of processor suppliers in order of precedence
    private static final List<ProcessorMatcher> PROCESSOR_MATCHERS = new ArrayList<>();

    static {
        // Register all available processor matchers in order of precedence
        PROCESSOR_MATCHERS.add(
                new ProcessorMatcher(
                        path -> path.contains("[*]"), () -> new ArrayJsonPathProcessor()));
        PROCESSOR_MATCHERS.add(
                new ProcessorMatcher(
                        path -> true, // Default matcher
                        () -> new JsonPathProcessorImpl()));
    }

    /**
     * Get the appropriate processor for a single JsonPath.
     *
     * @param jsonPath The JsonPath to process
     * @return The appropriate JsonPathProcessor
     */
    public static JsonPathProcessor getProcessor(JsonPath jsonPath) {
        return getProcessor(jsonPath.getPath());
    }

    /**
     * Get the appropriate processor for a JsonPath string.
     *
     * @param pathString The JsonPath string to process
     * @return The appropriate JsonPathProcessor
     */
    public static JsonPathProcessor getProcessor(String pathString) {
        for (ProcessorMatcher matcher : PROCESSOR_MATCHERS) {
            if (matcher.matches(pathString)) {
                return matcher.createProcessor();
            }
        }

        // Default to JsonPathProcessorImpl if no other processor matches
        return new JsonPathProcessorImpl();
    }

    /**
     * Get the appropriate processor for an array of JsonPaths with jsonFiledMissedReturnNull flag.
     *
     * @param paths Array of JsonPath objects
     * @param jsonFiledMissedReturnNull Whether to return null for missing fields
     * @return The appropriate JsonPathProcessor
     */
    public static JsonPathProcessor getProcessor(
            JsonPath[] paths, boolean jsonFiledMissedReturnNull) {
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException("JsonPath array cannot be null or empty");
        }

        JsonPathProcessor processor = getProcessor(paths[0]);

        // If this processor is a JsonPathProcessorImpl and jsonFiledMissedReturnNull is true,
        // we need to set the flag
        if (processor instanceof JsonPathProcessorImpl && jsonFiledMissedReturnNull) {
            ((JsonPathProcessorImpl) processor).setJsonFiledMissedReturnNull(true);
        }

        return processor;
    }

    /** Helper class to match and create JsonPathProcessors. */
    private static class ProcessorMatcher {
        private final PathMatcher matcher;
        private final Supplier<JsonPathProcessor> processorSupplier;

        public ProcessorMatcher(
                PathMatcher matcher, Supplier<JsonPathProcessor> processorSupplier) {
            this.matcher = matcher;
            this.processorSupplier = processorSupplier;
        }

        public boolean matches(String pathString) {
            return matcher.matches(pathString);
        }

        public JsonPathProcessor createProcessor() {
            return processorSupplier.get();
        }
    }

    /** Interface for path matching. */
    @FunctionalInterface
    private interface PathMatcher {
        boolean matches(String pathString);
    }
}

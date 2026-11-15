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

package org.apache.seatunnel.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/** Shared classpath-template loading and placeholder rendering for benchmark fixtures. */
public final class BenchmarkTemplates {

    private BenchmarkTemplates() {}

    /**
     * Loads a UTF-8 template from the benchmark module's classpath.
     *
     * @param resourceName absolute classpath resource name, for example {@code
     *     /benchmark/engine.yaml.template}
     * @return template text with a trailing newline
     * @throws IllegalStateException if the resource is missing or cannot be read
     */
    public static String load(String resourceName) {
        InputStream input = BenchmarkTemplates.class.getResourceAsStream(resourceName);
        if (input == null) {
            throw new IllegalStateException("Benchmark template was not found: " + resourceName);
        }
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n", "", "\n"));
        } catch (IOException e) {
            throw new IllegalStateException("Could not read benchmark template " + resourceName, e);
        }
    }

    /**
     * Replaces each {@code {{key}}} placeholder with its paired value.
     *
     * @param template template text to render
     * @param replacements alternating placeholder names and replacement values
     * @return rendered template with no unresolved placeholders
     * @throws IllegalArgumentException if replacements are not key-value pairs
     * @throws IllegalStateException if the rendered template still contains a placeholder
     */
    public static String render(String template, Object... replacements) {
        if (replacements.length % 2 != 0) {
            throw new IllegalArgumentException("Template replacements must be key-value pairs");
        }
        String rendered = template;
        for (int index = 0; index < replacements.length; index += 2) {
            String placeholder = "{{" + replacements[index] + "}}";
            if (rendered.contains(placeholder)) {
                rendered = rendered.replace(placeholder, String.valueOf(replacements[index + 1]));
            }
        }
        if (rendered.contains("{{")) {
            throw new IllegalStateException(
                    "Benchmark template contains an unresolved placeholder");
        }
        return rendered;
    }
}

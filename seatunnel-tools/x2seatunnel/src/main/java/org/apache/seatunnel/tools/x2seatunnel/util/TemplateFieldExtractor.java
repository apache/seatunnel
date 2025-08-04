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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Template field extractor - Extracts DataX field paths referenced in the template */
public class TemplateFieldExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TemplateFieldExtractor.class);

    // Regex for matching template variables: {{ datax.xxx }}
    private static final Pattern DATAX_VARIABLE_PATTERN =
            Pattern.compile("\\{\\{\\s*datax\\.([^}|\\s]+)(?:\\s*\\|[^}]*)?\\s*\\}\\}");

    /**
     * Extract all referenced DataX field paths from the template content
     *
     * @param templateContent The template content
     * @return The set of referenced DataX field paths
     */
    public Set<String> extractReferencedFields(String templateContent) {
        Set<String> referencedFields = new HashSet<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return referencedFields;
        }

        Matcher matcher = DATAX_VARIABLE_PATTERN.matcher(templateContent);

        while (matcher.find()) {
            String fieldPath = matcher.group(1); // Extract the part after datax.
            String normalizedPath = normalizeFieldPath(fieldPath);
            referencedFields.add(normalizedPath);

            logger.debug(
                    "Extracted template reference field: {} -> {}",
                    matcher.group(0),
                    normalizedPath);
        }

        logger.debug("Extracted {} referenced fields from the template", referencedFields.size());
        return referencedFields;
    }

    /**
     * Extract all referenced DataX field paths from multiple template contents
     *
     * @param templateContents Multiple template contents
     * @return The set of referenced DataX field paths
     */
    public Set<String> extractReferencedFields(String... templateContents) {
        Set<String> allReferencedFields = new HashSet<>();

        for (String templateContent : templateContents) {
            if (templateContent != null) {
                Set<String> fields = extractReferencedFields(templateContent);
                allReferencedFields.addAll(fields);
            }
        }

        logger.debug(
                "Extracted {} referenced fields from {} templates",
                templateContents.length,
                allReferencedFields.size());
        return allReferencedFields;
    }

    /**
     * Normalize the field path, converting the template path format to a format consistent with
     * DataX JSON paths
     *
     * @param fieldPath The original field path
     * @return The normalized field path
     */
    private String normalizeFieldPath(String fieldPath) {
        // In template: job.content[0].reader.parameter.username
        // Standardize as: job.content[0].reader.parameter.username
        // Return directly, as the template is already in correct format

        return fieldPath;
    }

    /**
     * Check if the template content contains DataX variable references
     *
     * @param templateContent The template content
     * @return Whether it contains DataX variable references
     */
    public boolean containsDataXReferences(String templateContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return false;
        }

        return DATAX_VARIABLE_PATTERN.matcher(templateContent).find();
    }

    /**
     * Get detailed information of all DataX variables in the template (including filters)
     *
     * @param templateContent The template content
     * @return The set of variable details
     */
    public Set<String> extractVariableDetails(String templateContent) {
        Set<String> variableDetails = new HashSet<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return variableDetails;
        }

        Matcher matcher = DATAX_VARIABLE_PATTERN.matcher(templateContent);

        while (matcher.find()) {
            String fullVariable = matcher.group(0);
            variableDetails.add(fullVariable);

            logger.trace("Extracted variable details: {}", fullVariable);
        }

        return variableDetails;
    }
}

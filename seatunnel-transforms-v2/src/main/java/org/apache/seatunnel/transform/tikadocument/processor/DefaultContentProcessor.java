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

package org.apache.seatunnel.transform.tikadocument.processor;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

/** Default implementation of ContentProcessor */
@Slf4j
public class DefaultContentProcessor implements ContentProcessor {

    private static final long serialVersionUID = 1L;

    private static final Pattern MULTIPLE_WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern EMPTY_LINES = Pattern.compile("(?m)^\\s*$");

    @Override
    public String processContent(
            String content,
            boolean removeEmptyLines,
            boolean trimWhitespace,
            boolean normalizeWhitespace,
            int minContentLength) {
        if (content == null) {
            return null;
        }

        String processedContent = content;

        try {
            // Remove empty lines if requested
            if (removeEmptyLines) {
                processedContent = removeEmptyLines(processedContent);
            }

            // Normalize whitespace if requested
            if (normalizeWhitespace) {
                processedContent = normalizeWhitespace(processedContent);
            }

            // Trim whitespace if requested
            if (trimWhitespace) {
                processedContent = processedContent.trim();
            }

            // Check minimum content length
            if (!isValidContent(processedContent, minContentLength)) {
                log.debug(
                        "Content does not meet minimum length requirement: {} < {}",
                        processedContent.length(),
                        minContentLength);
                return null;
            }

        } catch (Exception e) {
            log.error("Error processing content", e);
            return content; // return original content if processing fails
        }

        return processedContent;
    }

    @Override
    public boolean isValidContent(String content, int minLength) {
        if (content == null) {
            return minLength <= 0;
        }
        return content.length() >= minLength;
    }

    /** Remove empty lines from content */
    private String removeEmptyLines(String content) {
        if (content == null) {
            return null;
        }

        // Split by lines, filter out empty/whitespace-only lines, and rejoin
        String[] lines = content.split("\\r?\\n");
        StringBuilder result = new StringBuilder();

        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                if (result.length() > 0) {
                    result.append("\n");
                }
                result.append(line);
            }
        }

        return result.toString();
    }

    /** Normalize whitespace in content (replace multiple whitespace with single space) */
    private String normalizeWhitespace(String content) {
        if (content == null) {
            return null;
        }

        // Replace multiple consecutive whitespace characters with single space
        return MULTIPLE_WHITESPACE.matcher(content).replaceAll(" ");
    }
}

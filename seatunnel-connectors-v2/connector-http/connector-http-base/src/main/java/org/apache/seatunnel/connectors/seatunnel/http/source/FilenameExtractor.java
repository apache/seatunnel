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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilenameExtractor {

    private static final Pattern FILENAME_PATTERN =
            Pattern.compile("filename\\*?=([^;]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ENCODED_FILENAME_PATTERN =
            Pattern.compile("filename\\*=UTF-8''(.+?)(?:;|$)", Pattern.CASE_INSENSITIVE);

    private FilenameExtractor() {}

    public static String extract(String contentDisposition, String url) {
        // 1. Try Content-Disposition header
        if (contentDisposition != null && !contentDisposition.isEmpty()) {
            String filename = parseContentDisposition(contentDisposition);
            if (filename != null && !filename.isEmpty()) {
                return filename;
            }
        }

        // 2. Try URL path
        if (url != null && !url.isEmpty()) {
            String filename = extractFromUrl(url);
            if (filename != null && !filename.isEmpty()) {
                return filename;
            }
        }

        // 3. Default
        return "response-body-" + System.currentTimeMillis();
    }

    static String parseContentDisposition(String contentDisposition) {
        // Try RFC 5987 encoded filename first (filename*=UTF-8''xxx)
        Matcher encodedMatcher = ENCODED_FILENAME_PATTERN.matcher(contentDisposition);
        if (encodedMatcher.find()) {
            try {
                return java.net.URLDecoder.decode(encodedMatcher.group(1), "UTF-8");
            } catch (java.io.UnsupportedEncodingException e) {
                return encodedMatcher.group(1);
            }
        }

        // Try standard filename="xxx"
        Matcher matcher = FILENAME_PATTERN.matcher(contentDisposition);
        if (matcher.find()) {
            String filename = matcher.group(1).trim();
            if (filename.startsWith("\"") && filename.endsWith("\"")) {
                filename = filename.substring(1, filename.length() - 1);
            }
            return filename;
        }
        return null;
    }

    static String extractFromUrl(String url) {
        try {
            String path = URI.create(url).getPath();
            if (path != null && !path.isEmpty()) {
                int lastSlash = path.lastIndexOf('/');
                String filename = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
                if (!filename.isEmpty() && filename.contains(".")) {
                    return filename;
                }
            }
        } catch (IllegalArgumentException e) {
            // URL parsing failed, ignore
        }
        return null;
    }
}

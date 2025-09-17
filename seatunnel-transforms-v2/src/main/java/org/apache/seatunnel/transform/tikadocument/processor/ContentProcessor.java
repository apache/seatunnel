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

import java.io.Serializable;

/** Interface for processing extracted document content */
public interface ContentProcessor extends Serializable {

    /**
     * Process the extracted text content
     *
     * @param content raw text content from document extraction
     * @param removeEmptyLines whether to remove empty lines
     * @param trimWhitespace whether to trim whitespace
     * @param normalizeWhitespace whether to normalize whitespace
     * @param minContentLength minimum content length to consider valid
     * @return processed text content
     */
    String processContent(
            String content,
            boolean removeEmptyLines,
            boolean trimWhitespace,
            boolean normalizeWhitespace,
            int minContentLength);

    /**
     * Check if content meets minimum length requirements
     *
     * @param content text content to check
     * @param minLength minimum required length
     * @return true if content meets requirements, false otherwise
     */
    boolean isValidContent(String content, int minLength);
}

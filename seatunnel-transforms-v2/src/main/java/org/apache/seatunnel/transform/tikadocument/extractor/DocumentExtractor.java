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

package org.apache.seatunnel.transform.tikadocument.extractor;

import java.io.InputStream;

/** Interface for document content and metadata extraction */
public interface DocumentExtractor {

    /**
     * Extract metadata and content from document data
     *
     * @param documentData byte array containing the document
     * @param extractText whether to extract text content
     * @param extractMetadata whether to extract metadata
     * @param maxStringLength maximum length of extracted text
     * @return DocumentMetadata containing extracted information
     */
    DocumentMetadata extract(
            byte[] documentData, boolean extractText, boolean extractMetadata, int maxStringLength);

    /**
     * Extract metadata and content from document input stream
     *
     * @param inputStream input stream containing the document
     * @param extractText whether to extract text content
     * @param extractMetadata whether to extract metadata
     * @param maxStringLength maximum length of extracted text
     * @return DocumentMetadata containing extracted information
     */
    DocumentMetadata extract(
            InputStream inputStream,
            boolean extractText,
            boolean extractMetadata,
            int maxStringLength);

    /**
     * Check if the given MIME type is supported by this extractor
     *
     * @param mimeType MIME type to check
     * @return true if supported, false otherwise
     */
    boolean isSupported(String mimeType);

    /**
     * Detect the MIME type of the document
     *
     * @param documentData byte array containing the document
     * @return detected MIME type
     */
    String detectMimeType(byte[] documentData);

    /**
     * Set timeout for parsing operations
     *
     * @param timeoutMs timeout in milliseconds
     */
    void setTimeout(long timeoutMs);

    /** Clean up resources */
    void close();
}

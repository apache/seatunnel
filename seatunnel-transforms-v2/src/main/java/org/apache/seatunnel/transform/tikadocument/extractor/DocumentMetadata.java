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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/** Document metadata extracted by Tika */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentMetadata implements Serializable {

    /** Extracted text content from the document */
    private String content;

    /** Document title */
    private String title;

    /** Document author */
    private String author;

    /** Document creation date */
    private Date creationDate;

    /** Document modification date */
    private Date modificationDate;

    /** MIME type of the document */
    private String contentType;

    /** Detected language of the document */
    private String language;

    /** Number of pages (for documents that support page count) */
    private Integer pageCount;

    /** Document size in bytes */
    private Long fileSize;

    /** Document keywords */
    private String keywords;

    /** Document subject */
    private String subject;

    /** Additional metadata that wasn't specifically mapped */
    @Builder.Default private Map<String, Object> customMetadata = new HashMap<>();

    /** Whether the extraction was successful */
    private boolean successful;

    /** Error message if extraction failed */
    private String errorMessage;

    /** Get a metadata value by key */
    public Object getMetadataValue(String key) {
        switch (key.toLowerCase()) {
            case "content":
                return content;
            case "title":
                return title;
            case "author":
                return author;
            case "creation_date":
            case "created_at":
                return creationDate;
            case "modification_date":
            case "modified_at":
                return modificationDate;
            case "content_type":
            case "mime_type":
                return contentType;
            case "language":
            case "detected_language":
                return language;
            case "page_count":
            case "total_pages":
                return pageCount;
            case "file_size":
            case "document_size":
                return fileSize;
            case "keywords":
            case "document_keywords":
                return keywords;
            case "subject":
            case "document_subject":
                return subject;
            default:
                return customMetadata.get(key);
        }
    }

    /** Set a custom metadata value */
    public void setCustomMetadata(String key, Object value) {
        this.customMetadata.put(key, value);
    }
}

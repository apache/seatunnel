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

package org.apache.seatunnel.transform.tikadocument;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/** Error codes for TikaDocument Transform */
public enum TikaDocumentTransformErrorCode implements SeaTunnelErrorCode {
    SOURCE_FIELD_NOT_FOUND("TIKADOC-001", "Source field not found in input data"),
    SOURCE_FIELD_INVALID_TYPE("TIKADOC-002", "Source field must be byte array or base64 string"),
    UNSUPPORTED_DOCUMENT_FORMAT("TIKADOC-003", "Unsupported document format"),
    DOCUMENT_PARSING_FAILED("TIKADOC-004", "Document parsing failed"),
    DOCUMENT_PROCESSING_TIMEOUT("TIKADOC-005", "Document processing timed out"),
    INVALID_CONFIGURATION("TIKADOC-006", "Invalid transform configuration"),
    CONTENT_PROCESSING_FAILED("TIKADOC-007", "Content processing failed"),
    OUTPUT_FIELD_MAPPING_ERROR("TIKADOC-008", "Error mapping output fields");

    private final String code;
    private final String description;

    TikaDocumentTransformErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.common.ErrorHandleWay;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class TikaDocumentTransformConfig implements Serializable {

    // Source field configuration
    public static final Option<String> SOURCE_FIELD =
            Options.key("source_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Source field name containing document data (byte[] or base64 string)");

    // Output fields configuration
    public static final Option<Map<String, String>> OUTPUT_FIELDS =
            Options.key("output_fields")
                    .mapType()
                    .defaultValue(createDefaultOutputFields())
                    .withDescription("Map of output field names to their column names");

    // Parse options
    public static final Option<Boolean> EXTRACT_TEXT =
            Options.key("parse_options.extract_text")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to extract text content from documents");

    public static final Option<Boolean> EXTRACT_METADATA =
            Options.key("parse_options.extract_metadata")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to extract metadata from documents");

    public static final Option<Integer> MAX_STRING_LENGTH =
            Options.key("parse_options.max_string_length")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum length of extracted text content");

    // Content processing options
    public static final Option<Boolean> REMOVE_EMPTY_LINES =
            Options.key("content_processing.remove_empty_lines")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to remove empty lines from extracted text");

    public static final Option<Boolean> TRIM_WHITESPACE =
            Options.key("content_processing.trim_whitespace")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to trim whitespace from extracted text");

    public static final Option<Boolean> NORMALIZE_WHITESPACE =
            Options.key("content_processing.normalize_whitespace")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to normalize whitespace in extracted text");

    public static final Option<Integer> MIN_CONTENT_LENGTH =
            Options.key("content_processing.min_content_length")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Minimum content length to consider valid");

    // Error handling options
    public static final Option<String> ON_PARSE_ERROR =
            Options.key("error_handling.on_parse_error")
                    .stringType()
                    .defaultValue("skip")
                    .withDescription("How to handle parse errors: skip, fail, null");

    public static final Option<String> ON_UNSUPPORTED_FORMAT =
            Options.key("error_handling.on_unsupported_format")
                    .stringType()
                    .defaultValue("skip")
                    .withDescription("How to handle unsupported formats: skip, fail, null");

    public static final Option<Boolean> LOG_ERRORS =
            Options.key("error_handling.log_errors")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to log parsing errors");

    // Advanced options
    public static final Option<Integer> TIMEOUT_MS =
            Options.key("advanced.timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Parsing timeout in milliseconds");

    // Configuration properties
    private String sourceField;
    private Map<String, String> outputFields;
    private boolean extractText;
    private boolean extractMetadata;
    private int maxStringLength;
    private boolean removeEmptyLines;
    private boolean trimWhitespace;
    private boolean normalizeWhitespace;
    private int minContentLength;
    private ErrorHandleWay onParseError;
    private ErrorHandleWay onUnsupportedFormat;
    private boolean logErrors;
    private int timeoutMs;

    public static TikaDocumentTransformConfig of(ReadonlyConfig config) {
        TikaDocumentTransformConfig tikaConfig = new TikaDocumentTransformConfig();

        // Basic configuration
        tikaConfig.setSourceField(config.get(SOURCE_FIELD));
        tikaConfig.setOutputFields(config.get(OUTPUT_FIELDS));

        // Parse options
        tikaConfig.setExtractText(config.get(EXTRACT_TEXT));
        tikaConfig.setExtractMetadata(config.get(EXTRACT_METADATA));
        tikaConfig.setMaxStringLength(config.get(MAX_STRING_LENGTH));

        // Content processing
        tikaConfig.setRemoveEmptyLines(config.get(REMOVE_EMPTY_LINES));
        tikaConfig.setTrimWhitespace(config.get(TRIM_WHITESPACE));
        tikaConfig.setNormalizeWhitespace(config.get(NORMALIZE_WHITESPACE));
        tikaConfig.setMinContentLength(config.get(MIN_CONTENT_LENGTH));

        // Error handling
        tikaConfig.setOnParseError(parseErrorHandleWay(config.get(ON_PARSE_ERROR)));
        tikaConfig.setOnUnsupportedFormat(parseErrorHandleWay(config.get(ON_UNSUPPORTED_FORMAT)));
        tikaConfig.setLogErrors(config.get(LOG_ERRORS));

        // Advanced options
        tikaConfig.setTimeoutMs(config.get(TIMEOUT_MS));

        return tikaConfig;
    }

    private static ErrorHandleWay parseErrorHandleWay(String value) {
        switch (value.toLowerCase()) {
            case "skip":
                return ErrorHandleWay.SKIP;
            case "fail":
                return ErrorHandleWay.FAIL;
            case "null":
                return ErrorHandleWay.SKIP_ROW;
            default:
                return ErrorHandleWay.SKIP;
        }
    }

    private static Map<String, String> createDefaultOutputFields() {
        Map<String, String> defaultFields = new HashMap<>();
        defaultFields.put("content", "extracted_text");
        defaultFields.put("content_type", "mime_type");
        return defaultFields;
    }
}

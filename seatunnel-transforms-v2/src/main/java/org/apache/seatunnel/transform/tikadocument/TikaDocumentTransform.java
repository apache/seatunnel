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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.ErrorHandleWay;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.tikadocument.exception.TikaDocumentException;
import org.apache.seatunnel.transform.tikadocument.extractor.DocumentExtractor;
import org.apache.seatunnel.transform.tikadocument.extractor.DocumentMetadata;
import org.apache.seatunnel.transform.tikadocument.extractor.TikaDocumentExtractor;
import org.apache.seatunnel.transform.tikadocument.processor.ContentProcessor;
import org.apache.seatunnel.transform.tikadocument.processor.DefaultContentProcessor;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** TikaDocument Transform for extracting structured data from documents */
@Slf4j
public class TikaDocumentTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "TikaDocument";

    private final TikaDocumentTransformConfig config;
    private final DocumentExtractor extractor;
    private final ContentProcessor processor;
    private int sourceFieldIndex;
    private List<String> outputFieldNames;
    private List<SeaTunnelDataType<?>> outputFieldTypes;

    public TikaDocumentTransform(TikaDocumentTransformConfig config, CatalogTable catalogTable) {
        super(catalogTable, config.getOnParseError());
        this.config = config;
        this.extractor = new TikaDocumentExtractor();
        this.processor = new DefaultContentProcessor();

        // Set timeout
        this.extractor.setTimeout(config.getTimeoutMs());

        init();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void init() {
        // Find source field index
        try {
            sourceFieldIndex =
                    inputCatalogTable.getSeaTunnelRowType().indexOf(config.getSourceField());
        } catch (IllegalArgumentException e) {
            throw new TikaDocumentException.ConfigurationException(
                    "Source field '" + config.getSourceField() + "' not found in input data");
        }

        // Initialize output field metadata
        initOutputFields();
    }

    private void initOutputFields() {
        Map<String, String> outputFields = config.getOutputFields();
        outputFieldNames = new ArrayList<>();
        outputFieldTypes = new ArrayList<>();

        for (Map.Entry<String, String> entry : outputFields.entrySet()) {
            String fieldType = entry.getKey();
            String fieldName = entry.getValue();

            outputFieldNames.add(fieldName);
            outputFieldTypes.add(getSeaTunnelDataType(fieldType));
        }
    }

    private SeaTunnelDataType<?> getSeaTunnelDataType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "content":
            case "title":
            case "author":
            case "content_type":
            case "mime_type":
            case "language":
            case "detected_language":
            case "keywords":
            case "document_keywords":
            case "subject":
            case "document_subject":
                return BasicType.STRING_TYPE;
            case "creation_date":
            case "created_at":
            case "modification_date":
            case "modified_at":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "page_count":
            case "total_pages":
                return BasicType.INT_TYPE;
            case "file_size":
            case "document_size":
                return BasicType.LONG_TYPE;
            default:
                return BasicType.STRING_TYPE; // Default to string for unknown types
        }
    }

    @Override
    protected Column[] getOutputColumns() {
        Column[] columns = new Column[outputFieldNames.size()];
        for (int i = 0; i < outputFieldNames.size(); i++) {
            columns[i] =
                    PhysicalColumn.of(
                            outputFieldNames.get(i),
                            outputFieldTypes.get(i),
                            200,
                            true,
                            null,
                            "Extracted from document using TikaDocument transform");
        }
        return columns;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        try {
            // Get document data from source field
            Object sourceValue = inputRow.getField(sourceFieldIndex);
            byte[] documentData = extractDocumentData(sourceValue);

            if (documentData == null || documentData.length == 0) {
                return handleError("Source field contains null or empty data", null);
            }

            if (config.isLogErrors()) {
                log.debug("Processing document data, size: {} bytes", documentData.length);
            }

            // Extract document metadata
            DocumentMetadata metadata =
                    extractor.extract(
                            documentData,
                            config.isExtractText(),
                            config.isExtractMetadata(),
                            config.getMaxStringLength());

            if (!metadata.isSuccessful()) {
                return handleError(
                        "Document extraction failed: " + metadata.getErrorMessage(),
                        config.getOnParseError());
            }

            // Process content if extraction was successful
            if (metadata.getContent() != null && config.isExtractText()) {
                String processedContent =
                        processor.processContent(
                                metadata.getContent(),
                                config.isRemoveEmptyLines(),
                                config.isTrimWhitespace(),
                                config.isNormalizeWhitespace(),
                                config.getMinContentLength());
                metadata.setContent(processedContent);
            }

            // Build output values array
            Object[] outputValues = new Object[outputFieldNames.size()];
            for (int i = 0; i < outputFieldNames.size(); i++) {
                String fieldName = outputFieldNames.get(i);
                String fieldType = getFieldTypeByName(fieldName);
                outputValues[i] = metadata.getMetadataValue(fieldType);
            }

            return outputValues;

        } catch (Exception e) {
            log.error("Error processing document in TikaDocument transform", e);
            return handleError(
                    "Unexpected error during document processing: " + e.getMessage(),
                    config.getOnParseError());
        }
    }

    private String getFieldTypeByName(String fieldName) {
        // Map field name back to field type for metadata extraction
        for (Map.Entry<String, String> entry : config.getOutputFields().entrySet()) {
            if (entry.getValue().equals(fieldName)) {
                return entry.getKey();
            }
        }
        return fieldName; // fallback to field name itself
    }

    private byte[] extractDocumentData(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }

        if (sourceValue instanceof byte[]) {
            return (byte[]) sourceValue;
        }

        if (sourceValue instanceof String) {
            try {
                // Assume it's base64 encoded
                return Base64.getDecoder().decode((String) sourceValue);
            } catch (IllegalArgumentException e) {
                if (config.isLogErrors()) {
                    log.warn("Failed to decode base64 string, treating as raw text", e);
                }
                // Treat as raw text
                return ((String) sourceValue).getBytes();
            }
        }

        throw new TikaDocumentException.ConfigurationException(
                "Source field must be byte array or base64 string, got: "
                        + sourceValue.getClass().getSimpleName());
    }

    private Object[] handleError(String errorMessage, ErrorHandleWay errorHandleWay) {
        if (config.isLogErrors()) {
            log.warn("TikaDocument transform error: {}", errorMessage);
        }

        ErrorHandleWay handleWay =
                errorHandleWay != null ? errorHandleWay : config.getOnParseError();

        Object[] result = new Object[outputFieldNames.size()];

        switch (handleWay) {
            case FAIL:
                throw new TikaDocumentException(errorMessage);
            case SKIP:
                return null; // This will cause the row to be skipped
            case SKIP_ROW:
                // Fill with null values but keep the row
                for (int i = 0; i < result.length; i++) {
                    result[i] = null;
                }
                return result;
            default:
                // Fill with null values
                for (int i = 0; i < result.length; i++) {
                    result[i] = null;
                }
                return result;
        }
    }

    /** Clean up resources when transform is destroyed */
    public void close() {
        if (extractor != null) {
            extractor.close();
        }
    }

    /**
     * Override to add document metadata fields to the output catalog table's metadata schema. This
     * allows downstream transforms to access these fields as metadata.
     */
    @Override
    public CatalogTable getProducedCatalogTable() {
        // Get the base catalog table with physical columns
        CatalogTable baseCatalogTable = super.getProducedCatalogTable();

        // Build metadata schema with document extraction fields
        MetadataSchema.Builder metadataBuilder = MetadataSchema.builder();

        // Copy existing metadata from input if any
        if (inputCatalogTable.getMetadataSchema() != null
                && inputCatalogTable.getMetadataSchema().getColumns() != null) {
            for (Column column : inputCatalogTable.getMetadataSchema().getColumns()) {
                metadataBuilder.column((MetadataColumn) column);
            }
        }

        // Add document metadata fields to metadata schema
        addDocumentMetadataFields(metadataBuilder);

        MetadataSchema documentMetadataSchema = metadataBuilder.build();

        // Return catalog table with updated metadata schema
        return CatalogTable.withMetadata(baseCatalogTable, documentMetadataSchema);
    }

    /**
     * Add document extraction metadata fields to the metadata schema. These fields represent all
     * possible document metadata that can be extracted. Fields that don't exist in a particular
     * document will be null.
     */
    private void addDocumentMetadataFields(MetadataSchema.Builder metadataBuilder) {
        // Content field
        metadataBuilder.column(
                MetadataColumn.of(
                        "content",
                        BasicType.STRING_TYPE,
                        null,
                        true,
                        null,
                        "Extracted text content from the document"));

        // Content type / MIME type
        metadataBuilder.column(
                MetadataColumn.of(
                        "content_type",
                        BasicType.STRING_TYPE,
                        null,
                        true,
                        null,
                        "MIME type of the document"));

        // Title
        metadataBuilder.column(
                MetadataColumn.of(
                        "title", BasicType.STRING_TYPE, null, true, null, "Document title"));

        // Author
        metadataBuilder.column(
                MetadataColumn.of(
                        "author",
                        BasicType.STRING_TYPE,
                        null,
                        true,
                        null,
                        "Document author/creator"));

        // Subject
        metadataBuilder.column(
                MetadataColumn.of(
                        "subject", BasicType.STRING_TYPE, null, true, null, "Document subject"));

        // Keywords
        metadataBuilder.column(
                MetadataColumn.of(
                        "keywords", BasicType.STRING_TYPE, null, true, null, "Document keywords"));

        // Language
        metadataBuilder.column(
                MetadataColumn.of(
                        "language",
                        BasicType.STRING_TYPE,
                        null,
                        true,
                        null,
                        "Detected language of the document"));

        // Creation date
        metadataBuilder.column(
                MetadataColumn.of(
                        "created_date",
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        null,
                        true,
                        null,
                        "Document creation date"));

        // Modification date
        metadataBuilder.column(
                MetadataColumn.of(
                        "modified_date",
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        null,
                        true,
                        null,
                        "Document modification date"));

        // Page count
        metadataBuilder.column(
                MetadataColumn.of(
                        "page_count",
                        BasicType.INT_TYPE,
                        null,
                        true,
                        null,
                        "Number of pages in the document"));

        // File size
        metadataBuilder.column(
                MetadataColumn.of(
                        "file_size",
                        BasicType.LONG_TYPE,
                        null,
                        true,
                        null,
                        "Document size in bytes"));

        // All metadata as a map
        metadataBuilder.column(
                MetadataColumn.of(
                        "metadata",
                        new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
                        null,
                        true,
                        null,
                        "All document metadata as key-value pairs"));
    }
}

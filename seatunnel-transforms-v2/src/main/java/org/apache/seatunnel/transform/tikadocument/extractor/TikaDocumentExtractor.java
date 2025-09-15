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

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;

import org.xml.sax.SAXException;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/** Apache Tika implementation of DocumentExtractor */
@Slf4j
public class TikaDocumentExtractor implements DocumentExtractor {

    private final Parser parser;
    private final Detector detector;
    private long timeoutMs = 30000; // 30 seconds default timeout

    // Supported MIME types for first phase (MVP)
    private static final Set<String> SUPPORTED_MIME_TYPES =
            new HashSet<String>() {
                {
                    add("application/pdf");
                    add("application/msword");
                    add("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
                    add("application/vnd.ms-excel");
                    add("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                    add("application/vnd.ms-powerpoint");
                    add(
                            "application/vnd.openxmlformats-officedocument.presentationml.presentation");
                    add("text/plain");
                    add("text/html");
                    add("application/rtf");
                }
            };

    public TikaDocumentExtractor() {
        this.parser = new AutoDetectParser();
        this.detector = new DefaultDetector();
    }

    @Override
    public DocumentMetadata extract(
            byte[] documentData,
            boolean extractText,
            boolean extractMetadata,
            int maxStringLength) {
        try (InputStream inputStream = new ByteArrayInputStream(documentData)) {
            return extract(inputStream, extractText, extractMetadata, maxStringLength);
        } catch (IOException e) {
            log.error("Error creating input stream from document data", e);
            return DocumentMetadata.builder()
                    .successful(false)
                    .errorMessage("Failed to create input stream: " + e.getMessage())
                    .build();
        }
    }

    @Override
    public DocumentMetadata extract(
            InputStream inputStream,
            boolean extractText,
            boolean extractMetadata,
            int maxStringLength) {
        DocumentMetadata.DocumentMetadataBuilder builder = DocumentMetadata.builder();

        try (TikaInputStream tikaInputStream = TikaInputStream.get(inputStream)) {
            Metadata metadata = new Metadata();
            ParseContext parseContext = new ParseContext();
            parseContext.set(Parser.class, parser);

            String content = null;

            // Extract text content if requested
            if (extractText) {
                BodyContentHandler handler =
                        new BodyContentHandler(new WriteOutContentHandler(maxStringLength));

                long startTime = System.currentTimeMillis();
                parser.parse(tikaInputStream, handler, metadata, parseContext);
                long parseTime = System.currentTimeMillis() - startTime;

                if (parseTime > timeoutMs) {
                    log.warn(
                            "Document parsing took {} ms, which exceeds timeout of {} ms",
                            parseTime,
                            timeoutMs);
                }

                content = handler.toString();
            } else {
                // Parse only for metadata
                parser.parse(tikaInputStream, new BodyContentHandler(-1), metadata, parseContext);
            }

            // Build metadata object
            builder.successful(true).content(content);

            if (extractMetadata) {
                populateMetadata(builder, metadata);
            }

            // Set file size
            try {
                tikaInputStream.reset();
                builder.fileSize((long) tikaInputStream.available());
            } catch (IOException e) {
                log.debug("Could not determine file size", e);
            }

        } catch (TikaException e) {
            log.error("Tika parsing error", e);
            return builder.successful(false)
                    .errorMessage("Tika parsing failed: " + e.getMessage())
                    .build();
        } catch (IOException e) {
            log.error("IO error during document parsing", e);
            return builder.successful(false).errorMessage("IO error: " + e.getMessage()).build();
        } catch (SAXException e) {
            log.error("SAX parsing error", e);
            return builder.successful(false)
                    .errorMessage("SAX parsing failed: " + e.getMessage())
                    .build();
        } catch (Exception e) {
            log.error("Unexpected error during document parsing", e);
            return builder.successful(false)
                    .errorMessage("Unexpected error: " + e.getMessage())
                    .build();
        }

        return builder.build();
    }

    private void populateMetadata(
            DocumentMetadata.DocumentMetadataBuilder builder, Metadata metadata) {
        // Basic metadata
        builder.title(metadata.get(TikaCoreProperties.TITLE))
                .author(metadata.get(TikaCoreProperties.CREATOR))
                .subject(metadata.get(TikaCoreProperties.SUBJECT))
                .keywords(metadata.get("Keywords")) // Use string key for keywords
                .contentType(metadata.get(Metadata.CONTENT_TYPE))
                .language(metadata.get(TikaCoreProperties.LANGUAGE));

        // Dates
        Date creationDate = metadata.getDate(TikaCoreProperties.CREATED);
        if (creationDate != null) {
            builder.creationDate(creationDate);
        }

        Date modificationDate = metadata.getDate(TikaCoreProperties.MODIFIED);
        if (modificationDate != null) {
            builder.modificationDate(modificationDate);
        }

        // Page count (for PDFs and other documents that support it)
        String pageCountStr = metadata.get("xmpTPg:NPages");
        if (pageCountStr == null) {
            pageCountStr = metadata.get("meta:page-count");
        }
        if (pageCountStr != null) {
            try {
                builder.pageCount(Integer.parseInt(pageCountStr));
            } catch (NumberFormatException e) {
                log.debug("Could not parse page count: {}", pageCountStr);
            }
        }

        // Add all other metadata as custom metadata
        DocumentMetadata tempMetadata = builder.build();
        for (String name : metadata.names()) {
            if (!isStandardMetadata(name)) {
                tempMetadata.setCustomMetadata(name, metadata.get(name));
            }
        }
    }

    private boolean isStandardMetadata(String name) {
        return TikaCoreProperties.TITLE.getName().equals(name)
                || TikaCoreProperties.CREATOR.getName().equals(name)
                || TikaCoreProperties.SUBJECT.getName().equals(name)
                || "Keywords".equals(name) // Use string for keywords
                || TikaCoreProperties.LANGUAGE.getName().equals(name)
                || TikaCoreProperties.CREATED.getName().equals(name)
                || TikaCoreProperties.MODIFIED.getName().equals(name)
                || Metadata.CONTENT_TYPE.equals(name);
    }

    @Override
    public boolean isSupported(String mimeType) {
        if (mimeType == null) {
            return false;
        }
        return SUPPORTED_MIME_TYPES.contains(mimeType.toLowerCase());
    }

    @Override
    public String detectMimeType(byte[] documentData) {
        try (InputStream inputStream = new ByteArrayInputStream(documentData);
                TikaInputStream tikaInputStream = TikaInputStream.get(inputStream)) {

            Metadata metadata = new Metadata();
            MediaType mediaType = detector.detect(tikaInputStream, metadata);
            return mediaType.toString();

        } catch (IOException e) {
            log.error("Error detecting MIME type", e);
            return "application/octet-stream"; // fallback
        }
    }

    @Override
    public void setTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void close() {
        // No resources to clean up for AutoDetectParser
    }
}

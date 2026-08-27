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

package org.apache.seatunnel.transform.nlpmodel.embedding.multimodal;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;

/** Enumeration for multimodal modality types supported by embedding models */
@AllArgsConstructor
@Getter
@ToString
public enum ModalityType {
    TEXT("text", ModalityGroup.TEXT, Arrays.asList("text")),
    JPEG("jpeg", ModalityGroup.IMAGE, Arrays.asList("jpg", "jpeg")),
    PNG("png", ModalityGroup.IMAGE, Arrays.asList("png", "apng")),
    GIF("gif", ModalityGroup.IMAGE, Arrays.asList("gif")),
    WEBP("webp", ModalityGroup.IMAGE, Arrays.asList("webp")),
    BMP("bmp", ModalityGroup.IMAGE, Arrays.asList("bmp", "dib")),
    TIFF("tiff", ModalityGroup.IMAGE, Arrays.asList("tiff", "tif")),
    ICO("ico", ModalityGroup.IMAGE, Arrays.asList("ico")),
    ICNS("icns", ModalityGroup.IMAGE, Arrays.asList("icns")),
    SGI("sgi", ModalityGroup.IMAGE, Arrays.asList("sgi")),
    JPEG2000(
            "jpeg2000",
            ModalityGroup.IMAGE,
            Arrays.asList("j2c", "j2k", "jp2", "jpc", "jpf", "jpx")),

    MP4("mp4", ModalityGroup.VIDEO, Arrays.asList("mp4")),
    AVI("avi", ModalityGroup.VIDEO, Arrays.asList("avi")),
    MOV("mov", ModalityGroup.VIDEO, Arrays.asList("mov"));

    private final String name;
    private final ModalityGroup group;
    private final List<String> fileExtensions;

    public static ModalityType ofName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return null;
        }

        String trimmedName = name.trim().toLowerCase();
        for (ModalityType type : ModalityType.values()) {
            if (type.name.equalsIgnoreCase(trimmedName)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unsupported modality type: " + name.trim());
    }

    /**
     * Determine ModalityType from file extension/suffix If the value is not binary format, analyze
     * the file extension to determine the modality type
     */
    public static ModalityType fromFileSuffix(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String trimmedValue = value.trim().toLowerCase();
        String extension = "";
        int lastDotIndex = trimmedValue.lastIndexOf('.');
        if (lastDotIndex > 0 && lastDotIndex < trimmedValue.length() - 1) {
            extension = trimmedValue.substring(lastDotIndex + 1);
        }
        for (ModalityType type : ModalityType.values()) {
            if (type.fileExtensions.contains(extension)) {
                return type;
            }
        }
        return null;
    }

    /** Get all supported file extensions for this modality type */
    public List<String> getSupportedExtensions() {
        return fileExtensions;
    }

    /** Check if this modality type supports the given file extension */
    public boolean supportsExtension(String extension) {
        if (extension == null) {
            return false;
        }
        return fileExtensions.contains(extension.toLowerCase());
    }

    public enum ModalityGroup {
        IMAGE,
        VIDEO,
        TEXT
    }
}

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

/** Enumeration for multimodal modality types supported by embedding models */
@AllArgsConstructor
@Getter
@ToString
public enum ModalityType {
    TEXT("text"),
    IMAGE("image"),
    VIDEO("video");

    private final String name;

    public static ModalityType ofName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return TEXT;
        }
        for (ModalityType type : ModalityType.values()) {
            if (type.name.equalsIgnoreCase(name.trim().toLowerCase())) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported modality type: "
                        + name.trim()
                        + ". Supported types: "
                        + ModalityType.TEXT.getName()
                        + ", "
                        + ModalityType.IMAGE.getName()
                        + ", "
                        + ModalityType.VIDEO.getName());
    }
}
